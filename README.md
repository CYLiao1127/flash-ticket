# Flash Ticket: 高併發活動搶票與即時監控平台

Flash Ticket 是一套針對熱門開賣高併發場景設計的活動搶票與即時監控平台。後端以 PostgreSQL 作為訂單與座位狀態的唯一真實來源（搭配條件原子更新、`FOR UPDATE SKIP LOCKED`、`pg_advisory_xact_lock` 與部分唯一索引防止超賣），並透過 Transactional Outbox、Redis Pub/Sub 與 WebSocket/SSE 將座位異動與系統告警即時推播至前後台，同時支援在 512 MiB 記憶體限制內串流匯出 50 萬筆 Excel 銷售報表。

---

## 技術棧

- **後端框架**：FastAPI (Python 3.12) - 高效非同步 ASGI 框架
- **資料庫（唯一真實來源）**：PostgreSQL 16 - 搭配 `pg_advisory_xact_lock`、條件原子更新、`FOR UPDATE SKIP LOCKED` 與部分唯一索引 `uq_active_seat_order` 實作雙重防超賣控制
- **快取與訊息中介**：Redis 7 - `token_version` 快取、冪等鎖、每秒指標暫存、WebSocket / SSE 跨副本廣播
- **反向代理與負載均衡**：Nginx (Alpine) - 雙 API 副本負載均衡（`keepalive 128`）、邊緣限流保護、WebSocket 轉發、SSE 無緩衝代理、靜態前端託管與 Request ID 追蹤
- **容器化部署**：Docker & Docker Compose - 雙副本 API（各限制 `512M` 記憶體）+ 獨立背景 Worker
- **測試工具**：pytest, pytest-asyncio, k6

---

## 專案目錄結構

```text
flash-ticket/
├── app/                     # FastAPI 後端應用
│   ├── auth/                # 認證、JWT token_version 即時作廢、RBAC 與預設帳號 Seed
│   ├── core/                # Redis 連線、分散式 Request-ID 追蹤與結構化日誌
│   ├── events/              # 活動 CRUD、樂觀鎖、Keyset 分頁、座位快照與 10 萬筆 COPY 匯入
│   ├── tickets/             # 搶票選位／自動配位、單人 4 張限購、冪等鎖與 Outbox 序號
│   ├── orders/              # 訂單查詢與使用者取消訂單
│   ├── payments/            # 模擬金流、HMAC 簽章 Webhook、逾時釋放與遲到復活／退款
│   ├── realtime/            # WebSocket 訂閱、EventBroadcastHub 與斷線補發
│   ├── monitoring/          # 每秒指標收集、批次寫入、異常告警引擎、SSE 與 Admin 追蹤
│   ├── reports/             # 512 MiB 串流 Excel 報表（Server-side Cursor + constant_memory）
│   ├── workers/             # 背景 Worker（Leader Outbox Relay、逾時清理、退款、指標持久化）
│   ├── models.py            # PostgreSQL ORM 模型與 uq_active_seat_order 部分唯一索引
│   └── main.py              # FastAPI 進入點、路由掛載與中介層註冊
├── frontend/                # Nginx 託管之前台即時選位（index.html）與後台監控（admin.html）
├── migrations/              # Alembic 資料庫遷移腳本
├── loadtests/               # k6 壓測腳本（情境 A 搶票、情境 B 2,000 WS）與資料準備腳本
├── sample_data/             # 座位匯入範例檔（CSV / JSON 多元測資）
├── scripts/                 # 壓測後一致性校驗、Token 效能量測、50 萬筆報表基準腳本
├── tests/                   # 單元、整合、併發、故障恢復、授權矩陣與串流報表測試（83 項）
├── docs/                    # 資料庫設計文件（DATABASE.md）與實測存證（evidence/）
├── DESIGN.md                # 系統架構圖、設計假設取捨表與 Q1–Q6 完整設計問答
├── LOADTEST.md              # 壓力測試報告、瓶頸分析與一致性驗證結果
├── AI_USAGE.md              # AI 輔助開發分工與 6 處實戰問題修正紀錄
├── docker-compose.yml       # 多副本部署（Nginx + api-1/2 + Worker + PostgreSQL + Redis）
├── nginx.conf               # 負載均衡、邊緣限流、WebSocket 與無緩衝 SSE 設定
├── Makefile                 # 一行指令啟動、測試、壓測與一致性校驗捷徑
└── .env.example             # 環境變數範本與說明
```

---

## 啟動方式

### 1. Docker 部署（一鍵啟動全部服務）

可選步驟（若不複製，Docker Compose 會直接使用內建預設值）：

```bash
cp .env.example .env
```

一鍵建置並啟動全部服務（自動完成 PostgreSQL Migration 與三組預設測試帳號建立）：

```bash
docker compose up -d --build
```

服務啟動後各入口如下：

- **前台即時選位搶票介面**：`http://localhost:8000/`
- **後台即時監控與告警儀表板**：`http://localhost:8000/admin.html`
- **Swagger API 互動文件**：`http://localhost:8000/docs`
- **ReDoc API 文件**：`http://localhost:8000/redoc`
- **健康檢查端點**：`http://localhost:8000/health`

### 2. 本地運行步驟（基礎設施用 Docker，應用程式於本機執行）

若想在本機 Python 環境直接執行 FastAPI 與背景 Worker 進行開發除錯（需 Python 3.12）：

```bash
# 1. 僅啟動 PostgreSQL 與 Redis
docker compose up -d postgres redis

# 2. 建立虛擬環境並安裝依賴
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. 設定本機連線環境變數
export DATABASE_URL=postgresql+asyncpg://flash_user:flash_password@localhost:5432/flash_ticket
export SYNC_DATABASE_URL=postgresql://flash_user:flash_password@localhost:5432/flash_ticket
export REDIS_URL=redis://localhost:6379/0

# 4. 執行資料庫遷移與預設帳號初始化
alembic upgrade head && python -m app.auth.seed

# 5. 啟動 API（終端機 1）與背景 Worker（終端機 2）
uvicorn app.main:app --host 0.0.0.0 --port 8000
python -m app.workers.runner
```

*(註：直接啟動 Uvicorn 時可由 `http://localhost:8000/docs` 存取 API；若需透過 Nginx 託管靜態前端與邊緣限流，請使用上方 Docker Compose 啟動。)*

### 3. 執行測試與壓測

```bash
# 執行全套自動化測試
make test
# 或
docker compose --profile testing run --rm test

# 執行 Token 即時作廢與認證相依層效能基準測試 (Q5.1)
make benchmark-auth

# === k6 高併發壓力測試與一致性校驗 ===
# 1. 準備壓測資料（建立 10,000 座位活動與 3,000 組 JWT Token，有效期 60 分鐘）
make loadtest-prepare

# 2. 情境 A：高併發搶票壓測（3,000 VU / 10,000 座位 / 60 秒）
make loadtest-a
# （本機輕量穩態觀測版：300 VU / 30 秒）
docker run --rm --network flash-ticket_default -v "$PWD/loadtests:/loadtests" \
  -e TARGET_VUS=300 -e DURATION=30s grafana/k6 run --discard-response-bodies /loadtests/scenario_a_booking.js

# 3. 情境 B：2,000 條即時 WebSocket 訂閱連線壓測
make loadtest-b

# 4. 壓測後自動化資料一致性校驗（零超賣、單人 <= 4 張、Outbox 連號）
make verify-consistency
```

---

## 預設測試帳號

首次啟動時會自動建立以下測試帳號（**正式環境必須更換預設帳號密碼、`SECRET_KEY` 與 `PAYMENT_WEBHOOK_SECRET`**）：

- **Admin**：`admin@flashticket.com` / `AdminPass123!`
- **User**：`user@flashticket.com` / `UserPass123!`
- **Viewer**：`viewer@flashticket.com` / `ViewerPass123!`

---

## 核心 API 與即時協定摘要

完整 REST 規格見 Swagger（`http://localhost:8000/docs`）與 ReDoc（`http://localhost:8000/redoc`）。以下補充主要端點、錯誤慣例及 OpenAPI 未涵蓋的 WebSocket / SSE 協定：

| 模組 | 主要端點 | 權限 | 說明 |
| :--- | :--- | :--- | :--- |
| 認證 | `POST /api/v1/auth/{register,login,logout}`、`GET /api/v1/auth/me` | 公開 / 已登入 | JWT 內嵌 `token_version`，登出／停用／改角色即刻作廢 |
| 活動與座位 | `GET/POST /api/v1/events`、`PATCH/DELETE /api/v1/events/{id}`、`POST .../publish`、`GET .../seats`、`POST .../seats/import` | 公開或依角色 | Keyset 分頁、樂觀鎖 `version`、CSV/JSON 批量匯入 |
| 訂票與金流 | `POST /api/v1/events/{id}/tickets/{reserve,allocate}`、`GET /api/v1/orders`、`GET /api/v1/orders/{id}`、`POST /api/v1/orders/{id}/cancel`、`POST /api/v1/payments/mock/pay`、`POST /api/v1/payments/webhook` | User / Admin / 簽章 | 支援 `Idempotency-Key` 標頭；Webhook 採 HMAC-SHA256 + 5 分鐘防重放 |
| 監控與管理 | `GET /api/v1/admin/monitoring/{metrics,access-logs,alerts,trace/{request_id}}`、`GET/PATCH /api/v1/admin/users` | Admin / Viewer | 僅 Admin 可修改使用者角色與啟用狀態 |
| 串流報表 | `GET /api/v1/reports/exports?event_id=&date_from=&date_to=&batch_size=1000` | 全角色（依範圍） | User 僅限自己活動；Admin/Viewer 可看全部；回傳 `X-Report-Rows` |

- **統一錯誤格式與冪等慣例**：錯誤回應採 FastAPI 標準 `{"detail": "<message>"}`（`401` 未認證／憑證失效、`403` 越權、`409` 座位衝突／樂觀鎖衝突／冪等處理中、`422` 參數或冪等指紋不符、`429` Nginx 邊緣限流）。訂票端點攜帶 `Idempotency-Key` 重送時：已完成回 `200` 原訂單、處理中回 `409`、同 Key 但參數不同回 `422`。
- **WebSocket 即時座位推播（`WS /ws/events/{event_id}?token=<jwt>&last_seq=<n>`）**：
  - 握手通過後伺服器先發送 `{"type": "connected", "event_id": ..., "last_seq": ...}`。
  - 若提供 `last_seq` 且落後 `<= 48` 筆，依序補發遺漏事件；落後超過 48 筆則發送 `{"type": "resync_required", ...}` 通知前端重拉 `/seats` 快照。
  - 座位狀態變動事件格式為 `{"type": "seat_update", "event_type": "seat_held|seat_sold|seat_released", "sequence_id": <int>, "seat_ids": [...]}`；支援文字幀 `ping` ↔ `pong` 心跳，慢客戶端佇列滿（64 筆）時主動以 Close Code `1008` 關閉。
- **SSE 串流端點**：`GET /api/v1/events/{id}/imports/{job_id}/stream?token=<jwt>`（座位匯入即時進度）與 `GET /api/v1/admin/monitoring/alerts/stream?token=<jwt>`（Admin 告警串流），握手時先送 `event: connected`，後續推送 `event: progress` / `event: alert`（經 Nginx `proxy_buffering off` 即時送達）。

---

## 設計與測試紀錄

- **系統架構圖與設計問答（Q1–Q6 + 串流報表設計）**：[DESIGN.md](DESIGN.md)（架構圖、資料流與實作參考）
- **資料庫設計與 Schema 規範**：[docs/DATABASE.md](docs/DATABASE.md)（ERD、欄位說明、索引與狀態機）
- **AI 輔助開發日誌與除錯覆盤**：[AI_USAGE.md](AI_USAGE.md)（AI 輔助範圍及 6 個問題修正案例）
- **Token 即時作廢與認證相依層效能基準量測 (Q5.1)**：[docs/evidence/q5_token_benchmark.txt](docs/evidence/q5_token_benchmark.txt)（比較 JWT 驗證、Redis 檢查與資料庫查詢的延遲）
- **大量座位匯入與經 Nginx 無緩衝 SSE 實測紀錄**：[docs/evidence/seat_import_and_sse.txt](docs/evidence/seat_import_and_sse.txt)（100,000 列 PostgreSQL `COPY` 耗時與經 Nginx 逐幀即時送達實測）
- **雙重防超賣破壞性實驗與超賣復現存證 (Q1.3)**：[docs/evidence/q1_deliberate_failure.log](docs/evidence/q1_deliberate_failure.log)（實驗 A 拔除部分唯一索引仍擋住 30 併發；實驗 B 拔除條件更新重現 30× 超賣，分別對應 `4c7af9b` 與還原提交 `906a3b7`）
- **高併發壓力測試與一致性驗證報告**：[LOADTEST.md](LOADTEST.md)（情境 A/B 數據、WebSocket 連線池問題及資料一致性檢查）
- **512 MiB 容器 50 萬筆串流 Excel 匯出實測紀錄**：[docs/evidence/report_export_memory_profile.txt](docs/evidence/report_export_memory_profile.txt)（端對端 HTTP 500,000 列實測 `cgroup anon` 峰值 `95.68 MiB`、`oom_kill = 0`）

```mermaid
flowchart LR
    Client["前台 / 後台 / k6"] --> Nginx["Nginx\n(LB / 限流 / WS / SSE)"]
    Nginx --> API["api-1 & api-2\n(FastAPI, 各 512 MiB)"]
    API <--> PG[("PostgreSQL 16\n(SSOT + Outbox)")]
    API <--> Redis[("Redis 7\n(Token / 冪等 / 指標 / Pub/Sub)")]
    Worker["Background Worker\n(Leader Relay / 逾時釋放 / 指標持久化 / 告警)"] <--> PG
    Worker --> Redis
```

### 請求追蹤

每個 HTTP 請求都會帶有 `X-Request-ID`，並將此 ID 傳給後續的 Outbox Worker：

1. `request_tracing_middleware` 會接收或產生 UUID，存入 `contextvars`，並在回應標頭回傳 `X-Request-ID`。
2. `TracingFilter` 會在日誌中加入 `[instance_id]` 和 `[request_id]`：
   `[timestamp] [level] [instance_id] [request_id] [logger:line] message`
3. `create_outbox_event` 將 `request_id` 寫入 `event_outbox.payload`。
4. Worker 使用 `FOR UPDATE SKIP LOCKED` 取得待處理事件，恢復其中的 `request_id`。Redis Pub/Sub 發布成功後，才將事件標記為 `published`。

### 路由授權檢查

新增 API 時，CI 會檢查端點是否宣告授權方式（Q5.3）：

1. **強制授權宣告**：所有業務端點必須掛載 `get_current_user` / `require_roles(...)` 依賴，或顯式標註 `@public_endpoint(reason=...)` 並登錄於公開白名單。
2. **動態矩陣自動化驗證**：[tests/test_authorization.py](tests/test_authorization.py) 於 CI 階段自動遍歷 `app.routes` 解析依賴樹，針對每條受保護路由驗證匿名（401）、越權（403）與授權放行行為（角色矩陣檢查授權結果；其他業務行為由模組測試覆蓋）。
3. **GitHub Actions CI 自動執行**：[.github/workflows/ci.yml](.github/workflows/ci.yml) 於每次 `push` / `pull_request` 至 `main` 分支時自動啟動容器化測試套件，執行授權矩陣與 Alembic 相關測試。

---

## 功能清單與驗證狀態

### 使用者與權限管理

- [x] 使用者註冊、登入、登出
- [x] 登出、停用與角色變更時憑證立即作廢（Token Version 機制）
- [x] 管理員使用者管理 API（停用／啟用帳號與修改角色即時作廢憑證）
- [x] Token 失效效能量測與基準測試報告 (`d8c9926` / Q5.1)
- [x] RBAC 三角色權限控管（Admin、User、Viewer）
- [x] 系統自動建立種子帳號（Admin / User / Viewer）
- [x] 授權矩陣自動化驗證測試 (`ec2d927` / Q5.3)

### 活動與座位管理

- [x] 活動 CRUD（標題、描述、分類、場地、開賣/結束時間、區域與票價）
- [x] Keyset 分頁（防寫入時重複或遺漏）、篩選與排序
- [x] 樂觀鎖防並行編輯覆蓋
- [x] 已有訂單之活動禁止刪除保護
- [x] 活動狀態機（草稿 vs. 已發布）
- [x] 座位狀態快照 API（`GET /api/v1/events/{id}/seats`，含 `last_seq` 與分區統計）
- [x] 批量匯入座位（CSV / JSON，20MB / 10 萬筆 PostgreSQL `COPY` 寫入，附 `sample_data/` 範例）
- [x] 匯入模式支援（`all_or_nothing` 與 `skip_invalid`）
- [x] 重複檔案 SHA-256 去重（回傳既有 Job）
- [x] 匯入進度即時查詢與 SSE 進度推播、遺留任務恢復及錯誤列報告（`/imports/{job_id}`、`/stream`，含原列號與錯誤原因）
- [x] 建立者帳號停用時自動取消待執行與進行中之匯入任務並回滾未提交批次（`30e17f4` / Q5.2）
- [x] 即時座位狀態推播（Leader-Elected Outbox Relay + WebSocket 跨副本廣播 + 斷線補償與慢客戶端保護，Q4）

### 搶票與交易

- [x] 指定座位選位購買（條件原子更新 + 固定鎖順序防死結）
- [x] 區域配位（`SELECT FOR UPDATE SKIP LOCKED`，1~4 張）
- [x] 單人每場活動限購 4 張（原子級 `pg_advisory_xact_lock`）
- [x] 雙重防超賣保證（PostgreSQL 條件更新 + 部分唯一索引 `uq_active_seat_order`）
- [x] 冪等性處理（Idempotency Key：成功 200、處理中 409、參數不符 422）與原子 Outbox 活動序號 (`events.last_seq`)
- [x] 5% 服務費精確四捨五入（`Decimal` `ROUND_HALF_UP`）
- [x] 模擬金流服務與 HMAC Webhook 簽章回呼（5 分鐘防重放 + 事件冪等）
- [x] 逾時未付款自動釋放（DB-Clock `NOW()` 冪等釋放 Worker）
- [x] 逾時後 Webhook 到達的「復活 or 退款」事務處理（Q2.1）
- [x] 訂單查詢與使用者自主取消訂單

### 即時監控與維運

- [x] 每秒系統指標收集（RPS、p50/p95/p99 延遲、錯誤率、即時連線數）
- [x] 指標批次寫入資料庫與多副本時鐘偏差分析（Q6.3）
- [x] 存取日誌批次寫入資料庫
- [x] 系統面與業務面異常即時告警（5xx > 5%, p99 > 2s, Outbox > 1000, 業務失敗率 > 90%，含 SSE 即時推播與冷卻去重，Q6.2）
- [x] `X-Request-ID` 全鏈路追蹤與 Admin 跨服務日誌查詢（`/api/v1/admin/monitoring/trace/{request_id}`）
- [x] Admin 使用者管理（修改角色、啟用/停用狀態）

### 報表匯出

- [x] 活動銷售 Excel 報表匯出（`GET /api/v1/reports/exports`，含 `Summary` 摘要／區域彙總與 `Details` 訂單明細）
- [x] 50 萬筆訂單於 512MB 記憶體上限內串流匯出（磁碟備援 `xlsxwriter(constant_memory=True)` + PostgreSQL Server-Side Cursor，實測見 [`docs/evidence/report_export_memory_profile.txt`](docs/evidence/report_export_memory_profile.txt)）
- [x] 指定時區顯示（`Asia/Taipei`）與金額 Excel 數值型別
- [x] 匯出權限校驗（User 僅限自己活動、Viewer/Admin 可看全部）

### 前台與監控介面

- [x] 使用者即時座位圖與選位/搶票介面（Nginx 靜態託管：`http://localhost:8000/`，支援 WebSocket 即時變色與跳號補償）
- [x] 營運與管理者即時監控儀表板（Nginx 靜態託管：`http://localhost:8000/admin.html`，含 RPS/延遲趨勢圖、SSE 告警面板與 `X-Request-ID` 追蹤）

### 測試與壓力測試

- [x] 自動化測試：Docker Compose 全套 `make test` 單次實測 `83 passed, 0 failed`（2026-11-28；含新增的 Redis 舊版本快取回歸測試）。另有 2 個相依套件棄用警告。
- [x] 刻意破壞鎖機制的超賣復現存證（Q1 驗證，`4c7af9b` 破鎖復現 → `906a3b7` 還原）
- [x] 情境 A：3,000 VU、10,000 座位、持續負載 60 秒。Nginx 邊緣限流（`35r/s burst=15`）將約 72%（`8,444 / 11,708`）超載請求回 `429` 卸載，因此 60 秒內建立 2,217 筆訂單（3,782 席），5xx／逾時率為 `0.02%`（2 / 11,010；k6 顯示 `0.01%`），p50 / p95 / p99 為 `412.32 / 5,349.21 / 8,081.69 ms`；六項一致性檢查均通過（詳見 [`LOADTEST.md`](LOADTEST.md)）。
- [x] 情境 B 壓測（2,000 條即時 WebSocket 連線訂閱：於 `worker_connections 8192` 最終設定實測 **`100.00%` 連線成功率（`2,330 / 2,330`，Exit Code `0`、`ws_connecting` p50 `221.45ms` / p95 `3.15s` / p99 `5.81s`）**，伺服器端同時在線峰值 `2,002`，見 [`LOADTEST.md`](LOADTEST.md)）
- [x] 壓測後數據一致性校驗腳本（[`scripts/verify_consistency.py`](scripts/verify_consistency.py)，六項不變量全數通過）

---

## 已知限制與取捨

1. **單機 Docker 壓測環境之邊緣限流取捨**：在單機 8 vCPU 同時執行 k6（3,000 VU）+ Nginx + 雙 API + PostgreSQL + Redis 時，Nginx 透過 `limit_req`（`35r/s burst=15`）於邊緣以 `429 Too Many Requests` 快速回絕超出資料庫承受上限的瞬間寫入洪峰（約 72% 請求回 `429`），使這輪測試的 5xx／逾時率維持在 `0.02%`（2 / 11,010），六項資料一致性檢查均通過。這組限流值是單機測試環境的設定；正式環境需要依部署規模重新量測。
2. **50 萬筆串流 Excel 匯出之暫存磁碟需求**：因 XlsxWriter 須以磁碟暫存檔取代 `BytesIO` 才能真正啟用 `constant_memory=True`，單次匯出 50 萬筆明細期間容器 `/tmp` 需約 150–250 MB 可用磁碟空間，檔案串流完畢或連線中斷後會於 `finally` 區塊自動刪除。
3. **本機前端驗證方式**：前台與後台使用原生 HTML/JavaScript，由 Nginx 託管。若頁面和狀態管理之後變複雜，再考慮採用 SPA 框架。
4. **WebSocket 握手憑證的代理日誌風險**：瀏覽器握手將 JWT 放在 `?token=` 查詢參數；正式部署時應確認 Nginx 與外層代理不會記錄完整查詢字串，必要時加入遮罩。
5. **Outbox 保留策略尚未設定**：已發布事件目前不會定期清除，長期運作會持續增加 PostgreSQL 儲存量；清理前需先定義事件補發所需的保留範圍。
6. **存取日誌佇列不是耐久佇列**：Worker 會先從 Redis 移除一批日誌，再提交至 PostgreSQL；若程序剛好在兩步之間終止，該批日誌可能遺失。資料庫長時間不可用時，Redis 佇列也可能持續增長。
7. **未落庫監控指標有 24 小時恢復窗口**：Redis 秒桶 TTL 為 24 小時；Worker 或資料庫中斷超過此時間，或 Redis 資料遺失時，尚未寫入 PostgreSQL 的指標可能過期。
