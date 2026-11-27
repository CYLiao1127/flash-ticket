# AI 使用紀錄與問題修正

開發 Flash Ticket 時，我用 Antigravity（Gemini 代理式開發助手）協助撰寫 Docker 設定、資料庫模型、API、前台介面、測試和壓測腳本，也請它整理設計文件初稿。系統架構、資料庫鎖定方式、權限規則和驗收條件由我決定；合併前會檢查程式與測試結果。

以下記錄幾個初版實作中發現的問題，以及後來採取的修正。

## 1. 測試沒有執行 Alembic Migration

一開始的測試 fixture 用 `Base.metadata.create_all()` 建立資料表。這會直接根據 ORM 模型建表，沒有執行 Alembic migration；migration 即使漏掉欄位或索引，測試也可能照常通過。

後來改用 Alembic 的 `upgrade head` 建立測試資料庫，並新增 `test_alembic_migrations_and_schema_consistency()`，透過 `compare_metadata` 比對資料庫結構與 ORM 模型。

## 2. Request ID 沒有傳到 Worker

初版只在 HTTP middleware 處理 `X-Request-ID`。Outbox payload 沒有保存這個 ID，Worker 處理事件時也沒有把它放回日誌上下文，因此無法用同一個 ID 對照請求和背景工作。

修正後，`create_outbox_event` 會把 `request_id` 寫進 `event_outbox.payload`；Worker 取出事件時再綁回協程的 ContextVar。這樣可以從 HTTP 請求追到對應的 Worker 日誌。

## 3. Redis 快取可能保留舊的 Token 版本

初版在登出或停用帳號時先更新資料庫的 `token_version`，再更新 Redis。若 Redis 寫入失敗，快取仍可能保留舊版本；認證流程若只相信快取，就可能接受已撤銷的 Token。

現在認證以資料庫的 `token_version` 為準。Redis 更新失敗時會刪除該快取，後續請求再查資料庫確認版本。

## 4. Seed 會覆寫既有帳號設定

初版 seed 每次執行都會把既有測試帳號的密碼、角色和啟用狀態重設為預設值。管理者調整過的設定可能因此在重啟後被覆蓋。

現在只在帳號不存在時建立 seed 帳號；已存在的帳號不會被更新。

## 5. WebSocket 連線占用資料庫連線

初版的 `WS /ws/events/{event_id}` 透過 `Depends(get_db)` 取得資料庫 Session。連線 handler 會一直等待 WebSocket 訊息，因此 Session 也跟著長連線保留。雙 API 副本的連線池合計上限為 60 條；第一次 2,000 VU 測試的連線成功率只有 0.86%。

後來移除 WebSocket 路由上的 `Depends(get_db)`，只在握手認證與斷線補發資料時短暫開啟 Session。修正後重跑測試，連線成功率為 97.75%，伺服器端同時在線峰值為 2,002 條。數據與測試細節列在 [LOADTEST.md](LOADTEST.md)。

## 6. Excel 匯出沒有使用串流寫入

一版報表草稿用 `BytesIO`，同時設定 `in_memory=True` 和 `constant_memory=True`。XlsxWriter 的 `in_memory` 會覆蓋 `constant_memory`，大量資料因此仍可能留在記憶體。此外，查詢沒有 JOIN `order_items` 和 `seats`，無法取得報表需要的欄位。

修正後改用磁碟暫存檔、PostgreSQL 伺服器端游標和 SQL 聚合，並補上時區與數值格式。500,000 筆端對端匯出在 512 MiB 容器中的 `cgroup anon` 峰值為 95.68 MiB，`oom_kill = 0`；量測資料見 [q9_memory_profile.txt](docs/evidence/q9_memory_profile.txt)。
