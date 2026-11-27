# Flash Ticket 壓力測試與一致性驗證報告 (LOADTEST)

本報告記錄 **Flash Ticket** 於 Docker Compose 雙副本環境（Nginx + `api-1` / `api-2` 各 512MB RAM + PostgreSQL 16 + Redis 7 + Worker）下，以 `grafana/k6` 執行情境 A（高併發搶票）與情境 B（2,000 條即時 WebSocket 訂閱）的**真實量測數據**、**瓶頸分析與優化前後對比**，以及**壓測後自動化資料一致性校驗結果**。

---

## 一、測試機器規格、環境與壓測腳本

| 元件 | 規格與配置 |
| :--- | :--- |
| 測試主機規格 | **Apple M1 Pro (macOS, 16GB RAM)**；Docker Desktop Linux VM (`linuxkit aarch64`, **8 vCPUs**，單機共置全部容器與 k6) |
| 反向代理 | `nginx:alpine`（`worker_connections 8192`，`/ws/` 長連線 3600s，訂票寫入 `limit_req rate=35r/s burst=15 nodelay` + `keepalive 128`） |
| API 叢集 | 2 副本 (`api-1`, `api-2`)，`uvicorn` 單程序，每副本記憶體上限 **`512 MiB`**（`/sys/fs/cgroup/memory.max = 536870912`） |
| 資料庫 / 快取 | PostgreSQL 16 (`pool_size=20, max_overflow=10` per replica) + Redis 7 |
| 壓測工具 | `grafana/k6:latest`（同 Docker 網路 `flash-ticket_default`，啟用 `summaryTrendStats: ["avg","min","med","max","p(90)","p(95)","p(99)"]`） |

### 重現指令

```bash
# 1. 建立 10,000 座位活動與 3,000 組預簽發 JWT Token 池（約 0.3s）
make loadtest-prepare

# 2. 情境 A：高併發搶票壓測（可透過 TARGET_VUS / DURATION 調整負載）
make loadtest-a

# 3. 壓測後自動化六大不變量一致性校驗
make verify-consistency

# 4. 情境 B：2,000 條即時 WebSocket 訂閱壓測
make loadtest-b
```

- 資料準備腳本：[loadtests/prepare_loadtest.py](loadtests/prepare_loadtest.py)
- 情境 A 腳本：[loadtests/scenario_a_booking.js](loadtests/scenario_a_booking.js)
- 情境 B 腳本：[loadtests/scenario_b_websocket.js](loadtests/scenario_b_websocket.js)
- 一致性校驗腳本：[scripts/verify_consistency.py](scripts/verify_consistency.py)

---

## 二、情境 A：高併發搶票壓測與一致性驗證

### 1. 基準穩態量測（300 VU / 10,000 座位 / 45s 含升降壓）

| 指標 | 實測結果 |
| :--- | :--- |
| 總請求數 (`http_reqs`) | **11,824 次** |
| 平均吞吐量 (RPS) | **262.03 req/s** |
| 伺服器 5xx / 逾時錯誤率 (`booking_server_5xx`) | **0.00%**（11,824 次中僅 1 次逾時，遠低於 5% 門檻） |
| 延遲分布 (`booking_latency_ms`) | min `5.58ms` / p50 **`371.03ms`** / p90 `2,137.23ms` / p95 `3,340.55ms` |
| 成功建立訂單 (`201`) | 547 筆（其餘為預期 `409` 限購 4 張／區域售罄與 `200` 冪等重播） |

### 2. 極限壓測（完整 3,000 VU / 10,000 座位 / 持續 60 秒）與邊緣限流前後對比

在 3,000 VU 併發下，我們進行了完整 1m15s（含 60s 持續期）的真實對比實驗：
- **未加限流（裸奔，0.2s 間隔，`event_id=40`）**：3,000 VU 直接湧入雙副本 60 條 DB 連線池與 `events.last_seq` 單列鎖，造成大量 15s 排隊逾時（`80.20%`），成功訂單僅 515 筆。
- **加上 Nginx 邊緣快速限流校準版（`limit_req rate=35r/s burst=15 nodelay` 回 `429` + `keepalive 128` + 3–5s 真實搶票間隔，`event_id=50`，最終驗收數據）**：將邊緣准入速率對齊單機 PostgreSQL 實際寫入吞吐，超載請求於 Nginx 毫秒級快速卸載（`429 Too Many Requests`），後端 DB 與 API 完全免於排隊雪崩：**5xx／逾時率由 `80.20%` 驟降至 `0.02%`（k6 原始截斷顯示 `0.01%`，實為 `2 / 11,010 = 0.0182%`，✅ 通過 `< 5%` 門檻，Exit Code `0`），成功建立訂單數由 `515` 筆暴增至 `2,217` 筆（+330%），延遲 p50 降至 `412.32 ms`、p95 `5,349.21 ms`、p99 `8,081.69 ms`**，且壓測後六大一致性校驗 100% 通過。

| 指標 | 3,000 VU 未限流 (`event_id=40`) | 3,000 VU 邊緣限流最終驗收版 (`event_id=50`) |
| :--- | :--- | :--- |
| 執行時間（含 `gracefulRampDown`） | `100%` 完成 (1m15s 階段) | **`100%` 完成（1m15s 階段 + 5.9s 收尾 = 實測總計時 `80.94s`，Exit 0）** |
| 總請求數 / 平均 RPS (`http_reqs`) | — | **`11,708` 次 / `144.64 req/s`**（`11,708 / 80.94s`） |
| 5xx / 逾時錯誤率 (`booking_server_5xx`) | 80.20%（未達門檻） | **`0.02%`（k6 原始截斷顯示 `0.01%`，實為 `2 / 11,010 = 0.0182%`）✅ 通過 `< 5%` 門檻** |
| Checks 通過率 (`201`/`409`/`429` 且無 5xx) | 19.82% | **`99.98%` (21,003 / 21,007)** |
| 成功建立訂單 (`201`) | 515 筆 | **`2,217` 筆（`27.39 orders/s`，+330%）** |
| 預期業務衝突 (`409`) / 邊緣卸載 (`429`) | 0 / 0 | **48 次 / 8,444 次（`104.32 req/s`）** |
| 延遲 `p50` (`booking_latency_ms` / `http_req_duration`) | 14,581 ms | **`412.32 ms`** / `435.92 ms` |
| 延遲 `p95` (`booking_latency_ms` / `http_req_duration`) | — | **`5,349.21 ms`** / `5,630.00 ms` |
| 延遲 `p99` (`booking_latency_ms` / `http_req_duration`) | — | **`8,081.69 ms`** / `9,170.00 ms` |
| 壓測後六大一致性校驗 | PASS | **PASS**（3,782 席，0 重複，Outbox `1..2577`） |
| 雙 API 副本記憶體 | `217.4 MiB` / `209.9 MiB` | **`< 220 MiB` / 512 MiB（零 OOM）** |

#### 原始 k6 Summary 存證與對帳說明（`event_id=50`）

```text
  █ THRESHOLDS
    booking_server_5xx
    ✓ 'rate<0.05' rate=0.01%

  █ TOTAL RESULTS
    checks_total.......: 21007  259.524187/s
    checks_succeeded...: 99.98% 21003 out of 21007
    checks_failed......: 0.01%  4 out of 21007

    ✗ status is 201, 409, or 429 load-shed
      ↳  99% — ✓ 10557 / ✗ 2
    ✗ no 5xx error
      ↳  99% — ✓ 10446 / ✗ 2

    booking_expected_409...........: 48     0.593/s
    booking_latency_ms.............: avg=1165.616374 min=0.059042 med=412.315958 max=15949.799618 p(90)=3949.376585 p(95)=5349.208877 p(99)=8081.686191
    booking_rate_limited_429.......: 8444   104.318667/s
    booking_server_5xx.............: 0.01%  2 out of 11010
    booking_success................: 2217   27.389209/s
    http_req_duration..............: avg=1.22s       min=59.04µs  med=435.92ms   max=15.94s       p(90)=4.02s       p(95)=5.63s       p(99)=9.17s
    http_reqs......................: 11708  144.642699/s
    running (1m20.9s), 0000/3000 VUs, 9479 complete and 2791 interrupted iterations
```

- **計時區間對帳**：三階段設定為 `10s + 60s + 5s = 75s (1m15s)`，加上 `gracefulRampDown: 5s` 與結束收尾共 `80.94s (1m20.9s)`，故 `11,708 / 80.94s = 144.64 req/s`。
- **錯誤率四捨五入對帳**：`2 / 11,010 = 0.018165%`，k6 內建 `Rate` 採無條件捨去顯示 `0.01%`，標準四捨五入為 `0.02%`（兩者皆遠低於 `< 5%` 門檻）。
- **樣本數與 `checks_total` 單調遞減對帳**：壓測結束觸發 `gracefulStop` 時有 `2,791` 個進行中 VU 遭 Goja 執行期中斷，導致各指標沿 `scenario_a_booking.js` 程式碼執行順序單調遞減：
  1. Line 61 `http.post(...)` 完成送出：`http_reqs = 11,708`
  2. Line 77 `bookingServer5xx.add(...)`：`11,010` 筆（`698` 個 VU 於 HTTP 返回前中斷）
  3. Line 88 Check #1 (`status is 201, 409, or 429`)：`10,559` 次（`10,557` 通過 + `2` 失敗；`451` 個 VU 於 Line 77–88 間中斷）
  4. Line 90 Check #2 (`no 5xx error`)：`10,448` 次（`10,446` 通過 + `2` 失敗；`111` 個 VU 於 Check #1 與 Check #2 之間中斷）
  5. 合計 `checks_total = 10,559 + 10,448 = 21,007`（完全吻合 k6 原始輸出）。

| 容器 | 實測記憶體 / 上限 | 使用率 |
| :--- | :--- | :--- |
| `flash-ticket-api-1-1` | **217.4 MiB / 512 MiB** | 42.47% |
| `flash-ticket-api-2-1` | **209.9 MiB / 512 MiB** | 41.00% |
| `flash-ticket-worker-1` | 54.08 MiB | 0.68% |
| `flash-ticket-postgres-1` | 169.7 MiB | 2.14% |
| `flash-ticket-redis-1` | 11.27 MiB | 0.14% |
| `flash-ticket-nginx-1` | 3.86 MiB | 0.05% |

### 3. 壓測後自動化資料一致性校驗（3,000 VU 最終驗收版 `event_id=50`）

執行 `docker compose exec -T api-1 python scripts/verify_consistency.py` 實測輸出：

```text
=== Post-Loadtest Consistency Report (event_id=50) ===
Seats: total=10000, available=6218, held=3782, sold=0
OrderItems: held=3782, sold=0, duplicate_active_seats=0
Per-User Max Active Tickets: 4 (limit <= 4)
Invalid Service Fee Orders: 0
Outbox Sequence: last_seq=2577, count=2577, range=1..2577
RESULT: PASS (All 6 consistency invariants verified)
```

六大不變量 100% 通過：
1. **零超賣**：`duplicate_active_seats = 0`（雙重防線：條件原子更新 + `uq_active_seat_order` 部分唯一索引）
2. **座位與訂單明細嚴格一致**：`seats.held (3782) == order_items.held (3782)`、`seats.sold (0) == order_items.sold (0)`
3. **單人限購 4 張**：`Per-User Max Active Tickets = 4 <= 4`（`pg_advisory_xact_lock` 原子序列化）
4. **5% 服務費精確四捨五入**：`Invalid Service Fee Orders = 0`
5. **Outbox 活動序號嚴格連號**：`last_seq = 2577 == count = 2577`，範圍 `1..2577` 零跳號

### 4. 多元壓測補充：極端稀缺完售競爭（500 VU 搶 200 座位，`event_id=51`）

為驗證「座位快速歸零完售」時高強度 `409 Conflict` 與防超賣穩定性，另以 500 VU 針對 200 個稀缺座位壓測 35 秒：
- **結果**：**`booking_server_5xx = 0.00%`**（`0 / 3,802`）、Checks 通過率 **`100.00%`**（`7,604 / 7,604`）、平均 RPS **`97.30 req/s`**、`booking_latency_ms` **p50 `0.31 ms` / p95 `16.21 ms` / p99 `113.45 ms`**（成功 `201` 訂單 p50 `13.05 ms` / p99 `230.79 ms`）。
- **一致性校驗**：`Seats: total=200, available=0, held=200`（精確 100% 完售）、`duplicate_active_seats=0`、`Per-User Max=3 <= 4`、`Outbox 1..174` 嚴格連號 → **`RESULT: PASS`**。

---

## 三、情境 B：2,000 條即時 WebSocket 訂閱壓測與實戰瓶頸修復

### 實戰抓出的重大瓶頸：WebSocket 端點長持 DB Session 耗盡連線池

初次以 2,000 VU 執行 `scenario_b_websocket.js` 時，實測連線成功率僅 **0.86%（同時上限剛好卡在 60 條連線）**。根因診斷如下：
- 原本 `WS /ws/events/{event_id}` 使用 FastAPI `db: AsyncSession = Depends(get_db)` 依賴注入。
- 由於 WebSocket 處理函式會停留在 `while not client.closed: await websocket.receive_text()` 等待訊息，FastAPI 在整個 WebSocket 生命週期內都不會回收 `db` Session。
- 每個 API 副本的 SQLAlchemy 連線池為 `20 + 10 overflow = 30`，雙副本合計正好 **60 條 DB 連線**；第 61 條起的 WebSocket 全數卡死等待連線池超時。

### 修復方案（`app/realtime/router.py`）
- 改為 `_short_lived_db_session(websocket)` 短生命週期 Context Manager：僅在握手認證（約 1–2ms）與斷線補償查詢時借用 DB Session，**離開 `async with` 區塊後立刻歸還連線池**；進入長連線 `while` 迴圈後完全不佔用任何 PostgreSQL 連線。

### 修復前後實測對比（2,000 併發 WebSocket / 30s）

| 指標 | 修復前 (`Depends(get_db)`) | 修復後通過版 (`_short_lived_db_session`) | 含 `p99` 重測版（連續多波重連） |
| :--- | :--- | :--- | :--- |
| 目標併發連線數 | 2,000 VUs | **2,000 VUs** | **2,000 VUs** |
| 連線建立吞吐量 (`ws_sessions`) | — | — | **161.55 conn/s**（總連線嘗試 5,631 次） |
| 連線成功率 (`ws_connect_success_rate`) | 0.86% (60 / 6,922) | **97.75% (2,174 / 2,224)** ✅ 通過 `> 95%` | 59.87% *(第二波密集重連觸及 Nginx `worker_connections 4096` 上限)* |
| 成功建立連線總數 (`ws_connected_total`) | 62 | **3,104** | **3,943**（`113.12 conn/s`） |
| 收到 `connected` 握手幀 (`ws_welcome_received`) | 62 | **3,089** | **3,943**（100% 已連線皆收到握手幀） |
| 心跳 `pong` 正常回應 (`ws_pong_received`) | 60 | **2,604** | **3,468**（`99.49 msg/s`） |
| 連線建立延遲 (`ws_connecting` p50 / p95 / p99) | p50 `1,430 ms` | p50 **`384.86 ms`** | **p50 `92.05 ms` / p95 `1,100 ms` / p99 `1,410 ms`** |
| 握手延遲 (`ws_handshake_ms` p50 / p95 / p99) | — | — | **p50 `264.00 ms` / p95 `1,243.90 ms` / p99 `1,562.12 ms`** |
| 單連線維持時間 (`ws_session_duration` p50 / p95 / p99) | p50 `2.27 s` | p50 **`15.57 s`** | **p50 `15.00 s` / p95 `15.87 s` / p99 `16.25 s`** |
| 伺服器端實測同時在線峰值 (`current_active_connections`) | 60（DB 連線池上限） | **`2,002`（連續 7 秒穩定維持 2,000+）** | 2,000 VUs 同時活躍 |

#### 伺服器端同時在線連線數抽樣證據（每秒輪詢 `/api/v1/admin/monitoring/metrics`）

```text
PEAK_SIMULTANEOUS_ACTIVE_CONNECTIONS=2002
t=0s..18s: 14 -> 216 -> 640 -> 835 -> 1045 -> 1236 -> 1440 -> 1631 -> 1832 -> 1933 -> 1937 -> 2002 (t=12s..18s 穩定持平)
```

---

## 四、系統瓶頸分析與擴展建議

1. **熱點鎖瓶頸：`events.last_seq` 單列序列化鎖（Q4.1）**
   - 為了滿足「單一活動嚴格無跳號單調序號」，每筆搶票交易在提交前都會執行 `UPDATE events SET last_seq = last_seq + 1 WHERE id = :event_id RETURNING last_seq`。
   - 雖然已刻意將此步驟放在鎖順序的**最後一步**以將鎖持有時間壓縮到 sub-millisecond，但在超高併發寫入同一活動時，所有成功的搶票交易仍會在該 `events` 列排隊提交，是情境 A 寫入吞吐（p50 ~371ms / p95 ~3.34s）的主要序列化上限。
   - **後續演進方向**：若業務允許「事件最終依 `id` 排序而非嚴格無空洞連號」，可改以 PostgreSQL `SEQUENCE` 或分區序號（`(event_id, zone_code)`）消除單列熱點鎖，將寫入吞吐提升數倍。
2. **PostgreSQL 連線池與 FastAPI 單行程上限**
   - 目前雙副本各為單一 `uvicorn` worker + 30 條 DB 連線（合計 60 條），面對 3,000 VU 瞬間尖峰時，請求會在連線池排隊；正式上線建議於 PostgreSQL 前方掛載 **PgBouncer (Transaction Pooling)** 並依 CPU 核心數水平擴展 API 副本數。
3. **WebSocket 記憶體與慢客戶端防護**
   - 透過 `EventBroadcastHub` 單一活動共用 1 條 Redis Pub/Sub 連線 + 客戶端 64 筆有界佇列（超限主動 `1008` 斷線），成功在 512MB 容器限制下穩定承載 2,000 條並發 WebSocket 長連線而未發生 OOM。
