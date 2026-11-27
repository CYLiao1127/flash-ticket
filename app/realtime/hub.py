"""跨副本 WebSocket 廣播中心與慢客戶端保護機制 (Commit 20 / Q4)

核心設計：
1. 單一活動共用一條 Redis Pub/Sub 訂閱任務 (`event:{event_id}:updates`)，
   即使單台 API 副本同時掛載 2,000 條 WebSocket 連線，也只佔用 1 個 Redis 訂閱連線。
2. 訂閱就緒同步 (Subscribe-Before-Snapshot)：
   `connect()` 必須等待 `pubsub.subscribe(channel)` 完成後才返回，
   搭配路由層先 `connect()` 再讀取 DB `last_seq` / 補發查詢，杜絕訂閱與快照之間的事件空窗。
3. 斷線自動重連與失敗清理：
   Redis 監聽器遇錯後採指數退避自動重連並通知客戶端 `resync_required`；
   結束時必從 `_listener_tasks` 移除自身，`connect()` 亦會自動替換已結束（`done()`）的舊任務。
4. 慢客戶端保護 (Drop Slow Client)：
   當客戶端有界佇列已滿 (`QueueFull`) 或單次寫入逾時 (`WS_SEND_TIMEOUT_SECONDS`)，
   伺服器立即主動關閉該連線 (Close Code 1008) 並釋放記憶體，避免拖垮整台 API 造成 OOM。
"""

import asyncio
import json
import logging
from typing import Callable, Dict, Optional, Set

from fastapi import WebSocket

from app.core.redis import get_redis

logger = logging.getLogger("flash_ticket.realtime")

WS_CLIENT_MAX_QUEUE_SIZE = 64
WS_SEND_TIMEOUT_SECONDS = 5.0
WS_CLOSE_SLOW_CLIENT = 1008
REDIS_SUBSCRIBE_READY_TIMEOUT = 5.0
REDIS_RETRY_BASE_DELAY = 0.5
REDIS_RETRY_MAX_DELAY = 5.0


class ManagedWebSocketClient:
    """封裝單一 WebSocket 連線及其有界發送佇列"""

    def __init__(
        self,
        websocket: WebSocket,
        event_id: int,
        max_queue_size: int = WS_CLIENT_MAX_QUEUE_SIZE,
        send_timeout_seconds: float = WS_SEND_TIMEOUT_SECONDS,
        buffer_live_until_ready: bool = False,
    ) -> None:
        self.websocket = websocket
        self.event_id = event_id
        self.max_queue_size = max(1, int(max_queue_size))
        self.send_queue: asyncio.Queue[Optional[str]] = asyncio.Queue(
            maxsize=self.max_queue_size
        )
        self.send_timeout_seconds = send_timeout_seconds
        self.buffering_live = buffer_live_until_ready
        self._pending_live_buffer: list[str] = []
        self.closed = False
        self.dropped_reason: Optional[str] = None
        self.sender_task: Optional[asyncio.Task] = None

    def start_sender(self, on_disconnect) -> None:
        self.sender_task = asyncio.create_task(self._sender_loop(on_disconnect))

    async def _sender_loop(self, on_disconnect) -> None:
        try:
            while True:
                msg = await self.send_queue.get()
                if msg is None:
                    break
                await asyncio.wait_for(
                    self.websocket.send_text(msg),
                    timeout=self.send_timeout_seconds,
                )
        except asyncio.TimeoutError:
            self.dropped_reason = "send_timeout"
            logger.warning(
                f"Dropping slow WebSocket client on event_id={self.event_id} (send timeout)"
            )
            await self.close(code=WS_CLOSE_SLOW_CLIENT, reason="Slow client send timeout")
        except Exception:
            pass
        finally:
            self.closed = True
            await on_disconnect(self)

    def try_enqueue(self, message: str) -> bool:
        """非阻塞寫入客戶端有界佇列；佇列滿時回傳 False 交由 Hub 主動斷線保護"""
        if self.closed:
            return False
        try:
            self.send_queue.put_nowait(message)
            return True
        except asyncio.QueueFull:
            self.dropped_reason = "buffer_overflow"
            return False

    def enqueue_live(self, message: str) -> bool:
        """
        即時推播入口：
        - 若客戶端尚處於握手／快照補發階段 (`buffering_live=True`)，先暫存於有界緩衝區，
          避免即時事件搶在 `connected` 或 `catchup` 之前送出造成亂序。
        - 完成補發後直接寫入 `send_queue`。
        """
        if self.closed:
            return False
        if self.buffering_live:
            if len(self._pending_live_buffer) >= self.max_queue_size:
                self.dropped_reason = "buffer_overflow"
                return False
            self._pending_live_buffer.append(message)
            return True
        return self.try_enqueue(message)

    def flush_live_buffer(self, min_sequence_exclusive: int = 0) -> bool:
        """
        於 `connected` 與 `catchup` 寫入完成後同步（無 await、事件迴圈內原子執行）：
        1. 丟棄 `sequence_id <= min_sequence_exclusive` 的已包含事件（去重）。
        2. 依 `sequence_id ASC` 排序剩餘即時事件後依序排入 `send_queue`。
        3. 關閉 `buffering_live` 切換為正常即時推播模式。
        """
        buffered = self._pending_live_buffer
        self._pending_live_buffer = []
        self.buffering_live = False

        if not buffered:
            return True

        sequenced: list[tuple[int, int, str]] = []
        control: list[str] = []
        for idx, raw in enumerate(buffered):
            seq_val: Optional[int] = None
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and parsed.get("sequence_id") is not None:
                    seq_val = int(parsed["sequence_id"])
            except Exception:
                seq_val = None

            if seq_val is None:
                control.append(raw)
            elif seq_val > min_sequence_exclusive:
                sequenced.append((seq_val, idx, raw))

        sequenced.sort(key=lambda item: (item[0], item[1]))
        for _, _, raw in sequenced:
            if not self.try_enqueue(raw):
                return False
        for raw in control:
            if not self.try_enqueue(raw):
                return False
        return True

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self.closed:
            return
        self.closed = True
        try:
            await self.websocket.close(code=code, reason=reason)
        except Exception:
            pass

    async def stop_sender(self) -> None:
        self.closed = True
        if self.sender_task and not self.sender_task.done():
            self.sender_task.cancel()
            try:
                await self.sender_task
            except asyncio.CancelledError:
                pass


class EventBroadcastHub:
    """管理單一 API 副本上所有活動的 WebSocket 訂閱房間與 Redis Pub/Sub 橋接"""

    def __init__(
        self,
        *,
        redis_factory: Callable = get_redis,
        retry_base_delay: float = REDIS_RETRY_BASE_DELAY,
        retry_max_delay: float = REDIS_RETRY_MAX_DELAY,
    ) -> None:
        self._rooms: Dict[int, Set[ManagedWebSocketClient]] = {}
        self._listener_tasks: Dict[int, asyncio.Task] = {}
        self._listener_ready: Dict[int, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self._redis_factory = redis_factory
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay

    async def connect(
        self,
        event_id: int,
        websocket: WebSocket,
        *,
        max_queue_size: int = WS_CLIENT_MAX_QUEUE_SIZE,
        send_timeout_seconds: float = WS_SEND_TIMEOUT_SECONDS,
        start_redis_listener: bool = True,
        subscribe_timeout: float = REDIS_SUBSCRIBE_READY_TIMEOUT,
        buffer_live_until_ready: bool = False,
    ) -> ManagedWebSocketClient:
        client = ManagedWebSocketClient(
            websocket=websocket,
            event_id=event_id,
            max_queue_size=max_queue_size,
            send_timeout_seconds=send_timeout_seconds,
            buffer_live_until_ready=buffer_live_until_ready,
        )
        ready_event: Optional[asyncio.Event] = None
        async with self._lock:
            room = self._rooms.setdefault(event_id, set())
            room.add(client)
            if start_redis_listener:
                existing_task = self._listener_tasks.get(event_id)
                if existing_task is None or existing_task.done():
                    ready_event = asyncio.Event()
                    self._listener_ready[event_id] = ready_event
                    self._listener_tasks[event_id] = asyncio.create_task(
                        self._redis_event_listener(event_id, ready_event)
                    )
                else:
                    ready_event = self._listener_ready.get(event_id)

        client.start_sender(self.disconnect)

        # 等待 Redis Pub/Sub 訂閱確認生效後才返回；若逾時則清理客戶端並拋出 TimeoutError，
        # 避免尚未完成訂閱就回傳 connected 造成漏訊
        if ready_event is not None and not ready_event.is_set():
            try:
                await asyncio.wait_for(ready_event.wait(), timeout=subscribe_timeout)
            except asyncio.TimeoutError as exc:
                logger.warning(
                    f"Timed out waiting for Redis Pub/Sub subscription readiness on event_id={event_id}"
                )
                await client.stop_sender()
                await self.disconnect(client)
                raise TimeoutError(
                    f"Redis Pub/Sub subscription timed out for event_id={event_id}"
                ) from exc

        return client

    async def disconnect(self, client: ManagedWebSocketClient) -> None:
        listener_to_cancel: Optional[asyncio.Task] = None
        async with self._lock:
            room = self._rooms.get(client.event_id)
            if room and client in room:
                room.discard(client)
            if room is not None and len(room) == 0:
                self._rooms.pop(client.event_id, None)
                listener_to_cancel = self._listener_tasks.pop(client.event_id, None)
                self._listener_ready.pop(client.event_id, None)

        if listener_to_cancel and not listener_to_cancel.done():
            listener_to_cancel.cancel()
            try:
                await listener_to_cancel
            except asyncio.CancelledError:
                pass

    async def broadcast_raw(self, event_id: int, message: str) -> int:
        """將訊息廣播給該活動所有在線客戶端；若遇慢客戶端佇列溢位則立即主動斷線"""
        async with self._lock:
            clients = list(self._rooms.get(event_id, ()))

        delivered = 0
        slow_clients = []
        for client in clients:
            if client.enqueue_live(message):
                delivered += 1
            else:
                slow_clients.append(client)

        for slow in slow_clients:
            logger.warning(
                f"Dropping slow WebSocket client on event_id={event_id} "
                f"(reason={slow.dropped_reason or 'buffer_overflow'})"
            )
            await slow.close(code=WS_CLOSE_SLOW_CLIENT, reason="Slow client buffer overflow")
            await slow.stop_sender()
            await self.disconnect(slow)

        return delivered

    async def _redis_event_listener(
        self,
        event_id: int,
        ready_event: asyncio.Event,
    ) -> None:
        """
        活動專屬 Redis Pub/Sub 監聽器：
        - 訂閱生效後立即觸發 `ready_event`。
        - 若 Redis 連線中斷或發生異常，自動以指數退避重建訂閱並通知房間客戶端 `resync_required`。
        - 任務結束時必定清理 `_listener_tasks`，避免殘留已完成任務阻礙後續重啟。
        """
        channel = f"event:{event_id}:updates"
        retry_delay = self._retry_base_delay
        has_connected_once = False
        current_task = asyncio.current_task()

        try:
            while True:
                pubsub = None
                try:
                    redis = self._redis_factory()
                    pubsub = redis.pubsub()
                    await pubsub.subscribe(channel)
                    if not ready_event.is_set():
                        ready_event.set()
                    if has_connected_once:
                        # 重連成功後通知客戶端檢查缺口／重拉快照，避免中斷期間漏訊
                        await self.broadcast_raw(
                            event_id,
                            json.dumps(
                                {
                                    "type": "resync_required",
                                    "event_id": event_id,
                                    "reason": "pubsub_reconnected",
                                    "snapshot_url": f"/api/v1/events/{event_id}/seats",
                                }
                            ),
                        )
                    has_connected_once = True
                    retry_delay = self._retry_base_delay

                    while True:
                        msg = await pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=1.0
                        )
                        if msg and msg.get("type") == "message":
                            data = msg.get("data")
                            if isinstance(data, bytes):
                                data = data.decode("utf-8")
                            await self.broadcast_raw(event_id, str(data))
                        else:
                            await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(
                        f"Redis listener error on {channel}; reconnecting in {retry_delay:.2f}s: {exc}"
                    )
                    if self.active_client_count(event_id) == 0:
                        break
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, self._retry_max_delay)
                finally:
                    if pubsub is not None:
                        try:
                            await pubsub.unsubscribe(channel)
                            await pubsub.aclose()
                        except Exception:
                            pass
        finally:
            async with self._lock:
                if self._listener_tasks.get(event_id) is current_task:
                    self._listener_tasks.pop(event_id, None)
                    self._listener_ready.pop(event_id, None)

    def active_client_count(self, event_id: int) -> int:
        return len(self._rooms.get(event_id, ()))

    async def shutdown(self) -> None:
        """關閉所有房間的 Redis 訂閱任務與在線客戶端（於服務停機時呼叫）"""
        async with self._lock:
            tasks = list(self._listener_tasks.values())
            self._listener_tasks.clear()
            self._listener_ready.clear()
            all_clients = [c for room in self._rooms.values() for c in room]
            self._rooms.clear()

        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        for client in all_clients:
            await client.close(code=1001, reason="Server shutting down")
            await client.stop_sender()


event_hub = EventBroadcastHub()
