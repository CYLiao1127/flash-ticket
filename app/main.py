import asyncio
import time
import uuid
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from app.core.logging import setup_logging, request_id_ctx, request_user_holder_ctx
from app.core.redis import close_redis
from app.auth.router import router as auth_router, admin_users_router
from app.events.router import router as events_router
from app.tickets.router import router as tickets_router, admin_tickets_router
from app.payments.router import orders_router, payments_router
from app.monitoring.router import admin_monitoring_router
from app.realtime.hub import event_hub
from app.realtime.router import realtime_router, ws_router
from app.monitoring.collector import (
    ActiveConnectionASGIMiddleware,
    record_http_observation,
    refresh_active_connection_heartbeat,
)
from app.auth.dependencies import public_endpoint

logger = logging.getLogger("flash_ticket.api")
logger.setLevel(logging.INFO)


async def _active_conn_heartbeat_loop() -> None:
    """為長連線（SSE / WebSocket）每 10 秒續期節點活躍連線鍵 TTL"""
    while True:
        await asyncio.sleep(10)
        await refresh_active_connection_heartbeat()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 服務啟動初始化
    setup_logging()
    logger.info("Flash Ticket API service initializing...")
    heartbeat_task = asyncio.create_task(_active_conn_heartbeat_loop())
    try:
        yield
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        logger.info("Flash Ticket API service shutting down...")
        await event_hub.shutdown()
        await close_redis()


app = FastAPI(
    title="Flash Ticket API",
    description="高併發活動搶票與即時監控平台 API",
    version="1.0.0",
    lifespan=lifespan,
)

# 純 ASGI 連線追蹤中介層（涵蓋一般 HTTP、長連線 SSE 與 WebSocket 完整生命週期）
app.add_middleware(ActiveConnectionASGIMiddleware)

# 註冊 API 路由
app.include_router(auth_router)
app.include_router(admin_users_router)
app.include_router(events_router)
app.include_router(tickets_router)
app.include_router(admin_tickets_router)
app.include_router(orders_router)
app.include_router(payments_router)
app.include_router(admin_monitoring_router)
app.include_router(realtime_router)
app.include_router(ws_router)


@app.middleware("http")
async def request_tracing_middleware(request: Request, call_next):
    """
    全鏈路請求追蹤與監控採集中介軟體 (X-Request-ID & Per-Second Metrics Middleware)
    1. 若 Client 帶有 X-Request-ID 則繼承，否則由系統生成 UUID4
    2. 綁定至協程安全的 ContextVar 及 request.state
    3. 追蹤在線連線數與計算請求處理延遲 (Latency)
    4. 非同步寫入 Redis 秒級指標桶與存取日誌佇列（由背景 Worker 批次寫入 DB）
    5. 於回應標頭附加 X-Request-ID 回傳 Client
    """
    client_request_id = request.headers.get("X-Request-ID")
    req_id = client_request_id.strip() if client_request_id and client_request_id.strip() else uuid.uuid4().hex

    token = request_id_ctx.set(req_id)
    user_holder = {"user_id": None}
    user_token = request_user_holder_ctx.set(user_holder)
    request.state.request_id = req_id

    start_time = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Request-ID"] = req_id
        return response
    finally:
        latency_ms = (time.perf_counter() - start_time) * 1000
        client_ip = request.client.host if request.client else "unknown"
        user_id = user_holder["user_id"]
        await record_http_observation(
            request_id=req_id,
            user_id=user_id,
            method=request.method,
            path=request.url.path,
            status_code=status_code,
            latency_ms=latency_ms,
            client_ip=client_ip,
        )
        logger.info(
            f"{request.method} {request.url.path} - status={status_code} "
            f"latency={latency_ms:.2f}ms client_ip={client_ip}"
        )
        request_user_holder_ctx.reset(user_token)
        request_id_ctx.reset(token)


@app.get("/health", tags=["Health"])
@public_endpoint("Container health check probe")
async def health_check():
    """服務健康檢查探針"""
    return {"status": "ok"}
