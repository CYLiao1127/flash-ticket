# AI 工具使用與程式碼糾錯紀錄

本文件依據評測規範，記錄在開發「高併發活動搶票與即時監控平台」期間所使用的 AI 工具、應用範疇，以及在開發與 Code Review 審查過程中發現 AI 產出之架構盲點與程式碼缺陷的修正紀錄。

---

## 一、AI 工具使用概況

- **使用工具**：Gemini Code Assistant / LLM 輔助開發
- **主要應用範疇**：
  - 專案初始環境配置（Docker Compose、Dockerfile、Nginx 反向代理配置草擬）
  - 資料庫 SQLAlchemy ORM 模型與 Pydantic Request/Response Schema 骨架生成
  - 基礎 CRUD API 路由與型別標註生成
  - 壓測腳本（k6 / Locust）基礎範本撰寫

---

## 二、AI 產出程式碼缺陷與修正紀錄（精選案例）

### 案例 1：測試資料庫繞過 Migration（產生假性一致性的安全盲點）
- **AI 原先產出**：
  在編寫測試時，AI 於 pytest fixture 中直接調用 `Base.metadata.create_all()` 建立測試表結構。
- **問題分析（潛在風險）**：
  `Base.metadata.create_all()` 僅是將 Python ORM 類別轉為 DDL，完全繞過 Alembic 遷移腳本（`alembic upgrade head`）。若遷移腳本遺漏欄位、索引名稱拼錯或外鍵約束缺失，測試依然會全數通過，無法驗證真實 Migration 的正確性。
- **發現與修正方式**：
  **由 Code Review 及時指出該盲點**。將測試環境改以 Alembic 程式化指令（`upgrade head`）建表，並新增自動化測試 `test_alembic_migrations_and_schema_consistency()`，透過 `compare_metadata` 自動比對實體資料庫與 ORM 模型，達成零架構漂移（Zero Schema Drift）。

---

### 案例 2：全鏈路分散式追蹤斷層（未串連 Transactional Outbox 與背景 Worker）
- **AI 原先產出**：
  在實作分散式鏈路追蹤時，AI 僅編寫了 HTTP Middleware 來處理 `X-Request-ID`，而 Outbox 領域事件與 Worker runner 仍是 placeholder，僅用 `print()` 輸出，且 Outbox 未保存 Request ID。
- **問題分析（潛在風險）**：
  分散式追蹤的核心價值在於跨邊界串接。若僅在 HTTP 層產生 Trace ID，非同步背景 Worker 處理該事件時將失去原始請求上下文，日誌無法關聯，導致分散式排錯斷鏈。
- **發現與修正方式**：
  **由 Code Review 審查指出追蹤鏈路中斷**。重構 Outbox 寫入模組，將 ContextVar 中的 `request_id` 自動封裝至事件 payload；並在 Worker 輪詢取出事件時解開 ID 綁定回協程上下文，實現「Client $\rightarrow$ HTTP $\rightarrow$ DB Outbox $\rightarrow$ Worker」端到端鏈路閉環。

---

### 案例 3：Token 即時作廢時序漏洞（Redis 快取命中繞過資料庫檢查）
- **AI 原先產出**：
  在使用者登出與作廢 Token（`revoke_user_tokens`）時，AI 採用先更新資料庫 `token_version`、再寫入 Redis 快取的順序；且在認證中介層中，若 Redis 快取命中就只比對快取版本，不查驗資料庫。
- **問題分析（潛在風險）**：
  若更新 Redis 時發生暫態網路中斷或失敗，資料庫版本已遞增，但 Redis 仍保留舊版本。後續帶有已撤銷 Token 的請求若命中 Redis 快取，將因版本吻合而放行，導致「登出立即失效」的安全保證被穿透。
- **發現與修正方式**：
  **由 Code Review 審查指出快取與資料庫不一致的時序缺陷**。在認證流程中一律以資料庫實體版本為單一真理源（Single Source of Truth）；並在更新 Redis 異常時加入 fallback 機制直接執行 `redis.delete(key)`，強制後續請求回源查庫。

---

### 案例 4：Seed 種子帳號缺乏冪等性（每次啟動無差別覆寫維運設定）
- **AI 原先產出**：
  在自動建立預設種子帳號（`seed_users`）時，AI 邏輯為：若帳號已存在，則重新 hash 預設密碼、覆寫角色並強制將 `is_active` 設為 `True`。
- **問題分析（潛在風險）**：
  容器每次重啟或執行 migration 時皆會觸發 seed。若管理者在營運期間修改了預設帳號密碼、調整了角色權限，或基於安全考量停用帳號，重啟後將被無差別復原，造成嚴重的權限管理漏洞。
- **發現與修正方式**：
  **由 Code Review 審查指出非冪等副作用**。將種子帳號初始化改為嚴格冪等：若帳號已存在則僅輸出 log 提示並安全跳過，嚴禁覆寫任何現有密碼、角色及啟用狀態，完整尊重管理者的操作設定。
