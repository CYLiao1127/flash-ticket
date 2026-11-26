# Flash Ticket: 高併發活動搶票與即時監控平台

本專案為一套專為高併發搶購情境設計的「活動搶票平台」，並附帶供營運與管理人員使用的「即時流量監控與分析後台」。

---

## 一、技術棧說明

- **後端框架**：FastAPI (Python 3.12) - 高效非同步 ASGI 框架
- **資料庫（唯一真實來源）**：PostgreSQL 16 - 搭配行級鎖 (`FOR UPDATE SKIP LOCKED`)、條件原子更新與部分唯一索引保證零超賣
- **快取與訊息中介**：Redis 7 - Token 快取、即時指標暫存、WebSocket 跨副本廣播
- **反向代理與負載均衡**：Nginx (Alpine) - 雙 API 副本負載均衡、WebSocket 轉發、Request ID 追蹤
- **容器化部署**：Docker & Docker Compose - 支援多副本部署與記憶體限制 (512MB)
- **測試工具**：pytest, pytest-asyncio, k6

---

## 二、快速啟動與部署

### 1. 環境變數設定

本專案已提供完整的環境變數範本：

```bash
cp .env.example .env
```

### 2. 一鍵啟動全部服務（Docker Compose）

```bash
docker compose up -d --build
```

服務啟動後各入口如下：

- **Web 服務與 API 閘道**：`http://localhost:8000`
- **Swagger API 互動文件**：`http://localhost:8000/docs`
- **ReDoc API 文件**：`http://localhost:8000/redoc`
- **健康檢查端點**：`http://localhost:8000/health`

### 3. 一行指令執行自動化測試與基準壓測

```bash
# 執行全套自動化測試
make test
# 或
docker compose --profile testing run --rm test

# 執行 Token 即時作廢與認證相依層效能基準測試 (Q5.1)
make benchmark-auth
# 或
docker compose --profile testing run --rm test python scripts/benchmark_token_revocation.py
```

---

## 三、預設測試帳號（系統啟動自動建立）

系統於首次啟動或 Migration 完成後由種子模組自動寫入：

- **Admin**：`admin@flashticket.com` / `AdminPass123!`
- **User**：`user@flashticket.com` / `UserPass123!`
- **Viewer**：`viewer@flashticket.com` / `ViewerPass123!`

---

## 四、核心架構與設計文件

- **資料庫設計與 Schema 規範**：[docs/DATABASE.md](file:///Users/liao/Documents/flash-ticket/docs/DATABASE.md)（含完整 Mermaid ERD、欄位字典、防超賣部分索引與狀態機生命週期）
- **AI 輔助開發日誌與除錯覆盤**：[AI_USAGE.md](file:///Users/liao/Documents/flash-ticket/AI_USAGE.md)（記錄真實開發中 AI 產出錯誤與修正案例）
- **Token 即時作廢與認證相依層效能基準量測 (Q5.1)**：[docs/evidence/q5_token_benchmark.txt](file:///Users/liao/Documents/flash-ticket/docs/evidence/q5_token_benchmark.txt)（包含無狀態 JWT、Redis 快速失敗攔截、完整 `get_current_user` 與資料庫直查之延遲百分位數對比報告）

### 全鏈路分散式追蹤 (Distributed Tracing Architecture)
系統全面支援以 `X-Request-ID` 為核心的端到端分散式鏈路追蹤：
1. **HTTP 閘道層**：`request_tracing_middleware` 接收或生成 32 碼 UUID Trace ID，注入非同步安全之 `contextvars`，於回應標頭 `X-Request-ID` 返回客戶端。
2. **結構化日誌**：`TracingFilter` 自動為每一筆 LogRecord 注入 `[instance_id]` 與 `[request_id]`，輸出標準格式：
   `[timestamp] [level] [instance_id] [request_id] [logger:line] message`
3. **Transactional Outbox 領域事件**：`create_outbox_event` 將當前請求的 `request_id` 自動持久化至資料庫 `event_outbox.payload`。
4. **非同步背景 Worker**：Worker 輪詢取出事件時，自動自 payload 萃取 `request_id` 並綁定至協程 ContextVar，使非同步推播、重試與日誌無縫承接同一個 Trace ID，實現「Client $\rightarrow$ API 副本 $\rightarrow$ DB Outbox $\rightarrow$ Worker」的完全閉環。

### 宣告式授權與動態路由覆蓋防護 (Declarative RBAC & CI Guard)
為防止新增 API 端點時遺漏權限檢查（Q5.3）：
1. **強制授權宣告**：所有業務端點必須掛載 `get_current_user` / `require_roles(...)` 依賴，或顯式標註 `@public_endpoint(reason=...)` 並登錄於公開白名單。
2. **動態矩陣自動化驗證**：[tests/test_authorization.py](file:///Users/liao/Documents/flash-ticket/tests/test_authorization.py) 於 CI 階段自動遍歷 `app.routes` 解析依賴樹，針對每條受保護路由驗證匿名（401）、越權（403）與授權放行行為（角色矩陣驗證權限守門，業務邏輯則由各模組功能測試獨立覆蓋）。
3. **GitHub Actions CI 自動執行**：[.github/workflows/ci.yml](file:///Users/liao/Documents/flash-ticket/.github/workflows/ci.yml) 於每次 `push` / `pull_request` 至 `main` 分支時自動啟動容器化測試套件，確保上述授權矩陣與 Alembic 驗證不會被略過。

---

## 五、功能實作進度與未完成項目 (Feature Checklist)

### 1. 使用者與權限管理

- [x] 使用者註冊、登入、登出
- [x] 登出、停用與角色變更時憑證立即作廢（Token Version 機制）
- [x] 管理員使用者管理 API（停用／啟用帳號與修改角色即時作廢憑證）
- [x] Token 失效效能量測與基準測試報告 (Commit 05 / Q5.1)
- [x] RBAC 三角色權限控管（Admin、User、Viewer）
- [x] 系統自動建立種子帳號（Admin / User / Viewer）
- [x] 授權矩陣自動化驗證測試 (Commit 06 / Q5.3)

### 2. 活動與座位管理

- [ ] 活動 CRUD（標題、描述、分類、場地、開賣/結束時間、區域與票價）
- [ ] Keyset 分頁（防寫入時重複或遺漏）、篩選與排序
- [ ] 樂觀鎖防並行編輯覆蓋
- [ ] 已有訂單之活動禁止刪除保護
- [ ] 活動狀態機（草稿 vs. 已發布）
- [ ] 批量匯入座位（CSV / JSON，20MB / 10 萬筆 < 60 秒）
- [ ] 匯入模式支援（`all_or_nothing` 與 `skip_invalid`）
- [ ] 重複檔案 SHA-256 去重（回傳既有 Job）
- [ ] 匯入進度即時查詢與錯誤列報告（原列號 + 錯誤原因）
- [ ] 即時座位狀態推播（WebSocket / SSE）

### 3. 高併發搶票與交易

- [ ] 指定座位選位購買
- [ ] 區域配位（`SELECT FOR UPDATE SKIP LOCKED`，1~4 張）
- [ ] 單人每場活動限購 4 張（原子級 `pg_advisory_xact_lock`）
- [ ] 雙重防超賣保證（PostgreSQL 條件更新 + 部分唯一索引 `uq_active_seat_order`）
- [ ] 冪等性處理（Idempotency Key：成功 200、處理中 409、參數不符 422）
- [ ] 5% 服務費精確四捨五入（整數運算）
- [ ] 模擬金流服務與 HMAC Webhook 簽章回呼
- [ ] 逾時未付款自動釋放（DB-Clock `NOW()` 冪等釋放）
- [ ] 逾時後 Webhook 到達的「復活 or 退款」事務處理
- [ ] 訂單查詢與使用者自主取消訂單

### 4. 即時監控與維運

- [ ] 每秒系統指標收集（RPS、p50/p95/p99 延遲、錯誤率、即時連線數）
- [ ] 指標批次寫入資料庫與多副本時鐘偏差分析
- [ ] 存取日誌批次寫入資料庫
- [ ] 系統面與業務面異常即時告警（5xx > 5%, p99 > 2s, Outbox > 1000, 業務失敗率 > 90%）
- [ ] `X-Request-ID` 全鏈路追蹤與 Admin 跨服務日誌查詢
- [ ] Admin 使用者管理（修改角色、啟用/停用狀態）

### 5. 報表匯出

- [ ] 活動銷售 Excel 報表匯出（摘要、區域彙總、訂單明細）
- [ ] 50 萬筆訂單於 512MB 記憶體上限內串流匯出（`xlsxwriter` Constant Memory）
- [ ] 指定時區顯示（`Asia/Taipei`）與金額數值型別
- [ ] 匯出權限校驗（User 僅限自己活動、Viewer/Admin 可看全部）

### 6. 前台與監控介面

- [ ] 使用者即時座位圖與選位/搶票介面
- [ ] 營運與管理者即時監控儀表板

### 7. 驗證與壓力測試

- [ ] 一行指令執行全部測試（單元、整合、併發）
- [ ] 刻意破壞鎖機制的超賣復現存證（Q1 驗證）
- [ ] 情境 A 壓測（3,000 VU / 10,000 座位持續 60 秒）
- [ ] 情境 B 壓測（2,000 條即時 WebSocket 連線訂閱）
- [ ] 壓測後數據一致性校驗腳本（售出數 == 訂單票券數）
