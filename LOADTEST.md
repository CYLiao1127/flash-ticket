# Flash Ticket 壓力測試報告

這份報告整理 Docker Compose 雙 API 環境中的 k6 搶票與 WebSocket 測試結果，並記錄測試後的資料一致性檢查。

---

## 一、測試機器規格、環境與壓測腳本

| 元件 | 規格與配置 |
| :--- | :--- |
| 測試主機 | Apple M1 Pro、16GB RAM；Docker Desktop Linux VM（aarch64、8 vCPU），所有容器與 k6 在同一台主機執行 |
| 反向代理 | `nginx:alpine`（`worker_connections 8192`，`/ws/` 長連線 3600s，訂票寫入 `limit_req rate=35r/s burst=15 nodelay` + `keepalive 128`） |
| API 叢集 | 2 副本 (`api-1`, `api-2`)，`uvicorn` 單程序，每副本記憶體上限 **`512 MiB`**（`/sys/fs/cgroup/memory.max = 536870912`） |
| 資料庫 / 快取 | PostgreSQL 16 (`pool_size=20, max_overflow=10` per replica) + Redis 7 |
| 壓測工具 | `grafana/k6:latest`（同 Docker 網路 `flash-ticket_default`，啟用 `summaryTrendStats: ["avg","min","med","max","p(90)","p(95)","p(99)"]`） |

本報告所列最終情境 A 數據於 **2026-11-28（Asia/Taipei）** 執行；測試程式版本為 Git revision [`1b5e3aa`](https://github.com/CYLiao1127/flash-ticket/commit/1b5e3aa10c7a6109fad5c92a4d436030223780cf)。其他比較數據依各節所列情境與事件 ID 區分。

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
| 伺服器 5xx / 逾時錯誤率 (`booking_server_5xx`) | **0.01%**（`1 / 11,824 = 0.0085%`，1 次逾時納入錯誤率計算，k6 原始截斷顯示 `0.00%`，遠低於 5% 門檻） |
| 延遲分布 (`booking_latency_ms`) | min `5.58ms` / p50 **`371.03ms`** / p90 `2,137.23ms` / p95 `3,340.55ms` |
| 成功建立訂單 (`201`) | 547 筆（其餘為預期 `409` 限購 4 張／區域售罄與 `200` 冪等重播） |

### 2. 3,000 VU 搶票測試與限流前後比較

測試總長 1 分 15 秒，其中 60 秒維持目標負載：
- **未加限流（0.2 秒間隔，`event_id=40`）**：3,000 VU 同時向雙副本送出請求。測試環境的 DB 連線池上限為 60，`events.last_seq` 更新也會序列化同一活動的成功訂單；此輪逾時率為 `80.20%`，建立 515 筆訂單。
- **加上 Nginx 限流（`rate=35r/s burst=15 nodelay`，`event_id=50`）**：超出速率的請求由 Nginx 回覆 `429`。此輪 5xx／逾時率為 `0.02%`（2 / 11,010；k6 原始輸出截斷顯示 `0.01%`），建立 2,217 筆訂單，p50 / p95 / p99 為 `412.32 / 5,349.21 / 8,081.69 ms`；測試後六項資料一致性檢查均通過。這輪同時調整了限流、keepalive 與請求間隔，數據反映整組設定的結果。

| 指標 | 3,000 VU 未限流 (`event_id=40`) | 3,000 VU 邊緣限流最終驗收版 (`event_id=50`) |
| :--- | :--- | :--- |
| 執行時間（含 `gracefulRampDown`） | `100%` 完成 (1m15s 階段) | **`100%` 完成（1m15s 階段 + 5.9s 收尾 = 實測總計時 `80.94s`，Exit 0）** |
| 總請求數 / 平均 RPS (`http_reqs`) | — | **`11,708` 次 / `144.64 req/s`**（`11,708 / 80.94s`） |
| 5xx / 逾時錯誤率 (`booking_server_5xx`) | 80.20%（未達門檻） | **`0.02%`（k6 原始截斷顯示 `0.01%`，實為 `2 / 11,010 = 0.0182%`，通過 `< 5%` 門檻）** |
| Checks 通過率 (`201`/`409`/`429` 且無 5xx) | 19.82% | **`99.98%` (21,003 / 21,007)** |
| 成功建立訂單 (`201`) | 515 筆 | **`2,217` 筆（`27.39 orders/s`，+330%）** |
| 預期業務衝突 (`409`) / 邊緣卸載 (`429`) | 0 / 0 | **48 次 / 8,444 次（`104.32 req/s`）** |
| 延遲 `p50` (`booking_latency_ms` / `http_req_duration`) | 14,581 ms | **`412.32 ms`** / `435.92 ms` |
| 延遲 `p95` (`booking_latency_ms` / `http_req_duration`) | — | **`5,349.21 ms`** / `5,630.00 ms` |
| 延遲 `p99` (`booking_latency_ms` / `http_req_duration`) | — | **`8,081.69 ms`** / `9,170.00 ms` |
| 成功建立訂單 (`201`) 延遲 (`expected_response:true`) | — | **p50 `1,120 ms` / p95 `6,680 ms` / p99 `9,840 ms`**（僅含真正進入 DB 寫入之 201 請求） |
| 壓測後六大一致性校驗 | PASS | **PASS**（3,782 席，0 重複，Outbox `1..2577`） |
| 雙 API 副本記憶體 | `217.4 MiB` / `209.9 MiB` | **`< 220 MiB` / 512 MiB（零 OOM）** |

#### k6 輸出與數字核對（`event_id=50`）

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
      { expected_response:true }...: avg=1.92s       min=21.21ms  med=1.12s      max=12.38s       p(90)=5.01s       p(95)=6.68s       p(99)=9.84s
    http_reqs......................: 11708  144.642699/s
    running (1m20.9s), 0000/3000 VUs, 9479 complete and 2791 interrupted iterations
```

- **執行時間**：三階段設定為 `10s + 60s + 5s = 75s (1m15s)`，加上 `gracefulRampDown: 5s` 與結束收尾共 `80.94s (1m20.9s)`，故 `11,708 / 80.94s = 144.64 req/s`。
- **同機壓測端排程延遲說明**：整體延遲（`http_req_duration` p50 `435.92 ms`）包含約 72% 由 Nginx 直接回傳的 `429`（單純 Nginx 拒絕本應 `< 1 ms`），p50 仍達數百毫秒，顯示壓測端（k6）在 3,000 VU 時亦受同機 8 vCPU 資源競爭與排程延遲影響；因此成功訂單（`201`，`expected_response:true`）的延遲較能反映後端交易行為，但在同機環境下同樣可能偏高。
- **錯誤率計算**：`2 / 11,010 = 0.018165%`，k6 內建 `Rate` 採無條件捨去顯示 `0.01%`，標準四捨五入為 `0.02%`（兩者皆遠低於 `< 5%` 門檻）。
- **`checks_total` 與中斷迭代**：壓測結束觸發 `gracefulStop` 時有 `2,791` 個進行中 VU 遭 Goja 執行期中斷，導致各指標沿 `scenario_a_booking.js` 程式碼執行順序單調遞減：
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

六項一致性檢查結果：
1. **零超賣**：`duplicate_active_seats = 0`（雙層保護：條件原子更新 + `uq_active_seat_order` 部分唯一索引）
2. **鎖定中座位與訂單明細一致**：`seats.held (3782) == order_items.held (3782)`
3. **已售出座位與訂單明細一致**：`seats.sold (0) == order_items.sold (0)`
4. **單人限購 4 張**：`Per-User Max Active Tickets = 4 <= 4`（`pg_advisory_xact_lock` 原子序列化）
5. **5% 服務費精確四捨五入**：`Invalid Service Fee Orders = 0`
6. **Outbox 活動序號嚴格連號**：`last_seq = 2577 == count = 2577`，範圍 `1..2577` 零跳號

### 4. 多元壓測補充：極端稀缺完售競爭（500 VU 搶 200 座位，`event_id=51`）

為驗證「座位快速歸零完售」時高強度 `409 Conflict` 與防超賣穩定性，另以 500 VU 針對 200 個稀缺座位壓測 35 秒：
- **結果**：**`booking_server_5xx = 0.00%`**（`0 / 3,802`）、Checks 通過率 **`100.00%`**（`7,604 / 7,604`）、平均 RPS **`97.30 req/s`**、`booking_latency_ms` **p50 `0.31 ms` / p95 `16.21 ms` / p99 `113.45 ms`**（成功 `201` 訂單 p50 `13.05 ms` / p99 `230.79 ms`）。
- **一致性校驗**：`Seats: total=200, available=0, held=200`（200 個座位均已售出）、`duplicate_active_seats=0`、`Per-User Max=3 <= 4`、`Outbox 1..174` 嚴格連號 → **`RESULT: PASS`**。

---

## 三、WebSocket 連線測試（2,000 VU）

### 首次測試遇到的連線池問題

初次以 2,000 VU 執行 `scenario_b_websocket.js` 時，實測連線成功率僅 **0.86%（同時上限剛好卡在 60 條連線）**。根因診斷如下：
- 原本 `WS /ws/events/{event_id}` 使用 FastAPI `db: AsyncSession = Depends(get_db)` 依賴注入。
- 由於 WebSocket 處理函式會停留在 `while not client.closed: await websocket.receive_text()` 等待訊息，FastAPI 在整個 WebSocket 生命週期內都不會回收 `db` Session。
- 每個 API 副本的 SQLAlchemy 連線池為 `20 + 10 overflow = 30`，雙副本合計正好 **60 條 DB 連線**；第 61 條起的 WebSocket 全數卡死等待連線池超時。

### 調整方式（`app/realtime/router.py`）
- 改為 `_short_lived_db_session(websocket)` 短生命週期 Context Manager：僅在握手認證（約 1–2ms）與斷線補償查詢時借用 DB Session，**離開 `async with` 區塊後立刻歸還連線池**；進入長連線 `while` 迴圈後完全不佔用任何 PostgreSQL 連線。

### 調整前後的測試結果（2,000 VU / 30 秒）

*(註：`ws_connect_success_rate` 分母 `2,330` 統計完整跑完 15 秒連線視窗的 iterations；`ws_connected_total = 3,948` 另包含於 `gracefulStop` 階段中斷但已完成 `101` 升級並收到 `connected` 握手幀的 `1,618` 條連線。)*

| 指標 | 修復前 (`Depends(get_db)`) | 修復後 (`4096` 單輪抽樣版) | 最終設定通過版 (`worker_connections 8192` + `p99`) |
| :--- | :--- | :--- | :--- |
| 目標併發連線數 | 2,000 VUs | **2,000 VUs** | **2,000 VUs（Exit Code `0`）** |
| 連線建立吞吐量 (`ws_sessions`) | — | — | **112.75 conn/s**（總連線 3,948 次） |
| 連線成功率 (`ws_connect_success_rate`) | 0.86% (60 / 6,922) | 97.75% (2,174 / 2,224) | **`100.00%` (2,330 / 2,330，Checks `4,660 / 4,660`，通過 `> 95%`)** |
| 成功建立連線總數 (`ws_connected_total`) | 62 | 3,104 | **3,948**（`112.75 conn/s`） |
| 收到 `connected` 握手幀 (`ws_welcome_received`) | 62 | 3,089 | **3,948**（100% 收到握手幀） |
| 心跳 `pong` 正常回應 (`ws_pong_received`) | 60 | 2,604 | **2,824**（`80.65 msg/s`） |
| 連線建立延遲 (`ws_connecting` p50 / p95 / p99) | p50 `1,430 ms` | p50 `384.86 ms` | **p50 `221.45 ms` / p95 `3,150 ms` / p99 `5,810 ms`** |
| 握手延遲 (`ws_handshake_ms` p50 / p95 / p99) | — | — | **p50 `226.50 ms` / p95 `3,252.90 ms` / p99 `6,065.48 ms`** |
| 單連線維持時間 (`ws_session_duration` p50 / p95 / p99) | p50 `2.27 s` | p50 `15.57 s` | **p50 `15.20 s` / p95 `17.24 s` / p99 `19.81 s`** |
| 伺服器端實測同時在線峰值 (`current_active_connections`) | 60（DB 連線池上限） | **`2,002`（連續 7 秒維持 2,000+）** | 2,000 VUs 同時活躍 |

#### 伺服器端同時在線連線數抽樣證據（每秒輪詢 `/api/v1/admin/monitoring/metrics`）

```text
PEAK_SIMULTANEOUS_ACTIVE_CONNECTIONS=2002
t=0s..18s: 14 -> 216 -> 640 -> 835 -> 1045 -> 1236 -> 1440 -> 1631 -> 1832 -> 1933 -> 1937 -> 2002 (t=12s..18s 穩定持平)
```

---

## 四、系統瓶頸分析與擴展建議

1. **熱點鎖瓶頸：`events.last_seq` 單列序列化鎖（Q4.1）**
   - 為了滿足「單一活動嚴格無跳號單調序號」，每筆搶票交易在提交前都會執行 `UPDATE events SET last_seq = last_seq + 1 WHERE id = :event_id RETURNING last_seq`。
   - 雖然已刻意將此步驟放在鎖順序的**最後一步**以將鎖持有時間壓縮到 sub-millisecond，但在超高併發寫入同一活動時，所有成功的搶票交易仍會在該 `events` 列排隊提交，推測為情境 A 寫入延遲（300 VU p50 約 371ms、3,000 VU 成功 201 訂單 p50 約 1.12s）的主要瓶頸之一。
   - 若不要求活動序號連續且無空洞，可評估 PostgreSQL `SEQUENCE` 或依 `(event_id, zone_code)` 分區序號，減少所有訂單更新同一 `events` 列造成的等待。
2. **PostgreSQL 連線池與 FastAPI 單行程上限**
   - 目前雙副本各為單一 `uvicorn` worker + 30 條 DB 連線（合計 60 條），面對 3,000 VU 瞬間尖峰時，請求會在連線池排隊；正式上線建議於 PostgreSQL 前方掛載 **PgBouncer (Transaction Pooling)** 並依 CPU 核心數水平擴展 API 副本數。
3. **WebSocket 記憶體與慢客戶端防護**
   - `EventBroadcastHub` 讓同一活動共用一條 Redis Pub/Sub 連線；每個客戶端佇列上限為 64 筆，超過上限會以 `1008` 中斷連線。該測試在 512 MiB 容器中同時維持約 2,000 條連線，未觀察到 OOM。
