# AI 工具使用與程式碼糾錯紀錄

本文件依據測試規範，記錄在開發高併發搶票與即時監控平台期間所使用的 AI 工具、應用範疇，並詳實記錄真實開發中 AI 產生的程式碼與架構設計錯誤、發現過程及修正方案。

---

## 一、AI 工具使用概況

- **使用工具**：Gemini Code Assistant / LLM 輔助開發
- **主要應用範疇**：
  - 專案初始環境配置（Docker Compose, Dockerfile, Nginx 反向代理）
  - 資料庫 Schema 與 Pydantic 模型草擬
  - 基礎 CRUD API 路由與型別標註生成
  - 壓測腳本（k6 / Locust）基礎模板撰寫

---

## 二、AI 產出錯誤與修正紀錄（真實案例）

### 案例 1：Nginx 容器 Port Mapping 拼寫錯誤導致外部連線失敗
- **AI 原先產出**：
  在配置 `docker-compose.yml` 時，AI 將 Nginx 的連接埠對應寫為：
  ```yaml
  ports:
    - "${APP_PORT:-8000}:800"
  ```
- **問題分析（為何有誤）**：
  `nginx.conf` 內部監聽的是 HTTP 標準埠 `80`。將 Host 的 8000 對應到容器內的 800 埠，Docker Compose 本身語法不會報錯，但外部 HTTP 流量完全無法抵達 Nginx，導致「服務看起來 healthy 但所有請求皆 Connection Refused」。
- **發現與修正方式**：
  在審查 C01 配置時由開發者及時抓出。立即將端口映射修正為 `"${APP_PORT:-8000}:80"`。

---

### 案例 2：Python 3.12 下 `passlib` 與最新 `bcrypt` 版本不相容
- **AI 原先產出**：
  在 `requirements.txt` 中，AI 僅宣告了 `passlib[bcrypt]==1.7.4`，未對底層 `bcrypt` 進行版本鎖定。
- **問題分析（為何有誤）**：
  `passlib 1.7.4` 依賴 `bcrypt.__about__` 讀取元數據。當 pip 自動解析安裝最新的 `bcrypt>=4.1` 時，該屬性已被移除，會在執行密碼 hash 或驗證時拋出 `AttributeError: module 'bcrypt' has no attribute '__about__'` 致命錯誤。
- **發現與修正方式**：
  在環境依賴審查時識別出此常見相容性斷層。修正方式為在 `requirements.txt` 中明確向下固定相容版本 `bcrypt==4.0.1`，保證密碼驗證模組運作正常。

---

### 案例 3：雙副本平行啟動導致資料庫 Migration 競爭與 502 閘道錯誤
- **AI 原先產出**：
  AI 在多副本架構中，將 `api-1` 與 `api-2` 的啟動指令皆設為直接執行 `uvicorn`，且 Nginx 僅等待容器啟動（而非健康檢查通過）。
- **問題分析（為何有誤）**：
  1. 若兩個 API 副本在啟動階段平行執行 Alembic migration 與 seed 資料寫入，會發生 DDL/DML 鎖競爭甚至失敗。
  2. Nginx 若在 FastAPI ASGI 服務就緒前就轉發請求，會在部署瞬間回傳 HTTP 502 Bad Gateway。
- **發現與修正方式**：
  重構 `docker-compose.yml` 拓撲：
  1. 抽出獨立的單次執行容器 `migrate`（`restart: "no"`），負責執行 `alembic upgrade head && python -m app.auth.seed`。
  2. `api-1` 與 `api-2` 明確宣告 `depends_on: migrate: condition: service_completed_successfully`。
  3. 為 API 實例加上 `/health` 探針檢查，Nginx 必須等待 API `condition: service_healthy` 才開始轉發流量。


---

### 案例 4：測試資料庫使用 `Base.metadata.create_all()` 繞過 Migration，無法驗證實際 Schema 一致性
- **AI 原先產出**：
  在編寫 `tests/test_db_schema.py` 時，AI 在 pytest fixture 中直接調用 `Base.metadata.drop_all()` 與 `Base.metadata.create_all()` 來建立測試環境。
- **問題分析（為何有誤）**：
  使用 `Base.metadata.create_all()` 僅是將 Python ORM 定義轉成 DDL，完全沒有執行 Alembic 遷移腳本（`alembic upgrade head`）。這會導致「Alembic migration 即使遺漏欄位、索引缺失或約束名稱打錯，pytest 測試依然能全數通過」的假性安全感，失去 CI/CD 自動化驗證資料庫遷移的防禦價值。
- **發現與修正方式**：
  由 Code Review 及時指出該盲點。我們進行了兩項關鍵重構：
  1. 將測試環境改為使用 Alembic 程式化指令（`command.downgrade(cfg, 'base')` 與 `command.upgrade(cfg, 'head')`）建立實體表結構，所有資料庫測試皆在真實 migration 生成的 schema 上執行。
  2. 增加專屬自動化測試 `test_alembic_migrations_and_schema_consistency()`，透過 `alembic.autogenerate.compare_metadata` 自動比對實體資料庫與 `Base.metadata`，確保 Alembic 遷移腳本與 SQLAlchemy ORM 模型保持絕對零差異（Zero Schema Drift）。


---

### 案例 5：Alembic `fileConfig` 靜默設置 `disable_existing_loggers=True` 導致背景 Worker Logger 被停用
- **AI 原先產出**：
  在 `migrations/env.py` 中，AI 沿用 Alembic 範本預設的配置調用：
  ```python
  if config.config_file_name is not None:
      fileConfig(config.config_file_name)
  ```
- **問題分析（為何有誤）**：
  Python 標準庫 `logging.config.fileConfig` 預設帶有隱藏參數 `disable_existing_loggers=True`。當測試或服務啟動執行 Alembic 遷移腳本時，所有在此之前已經被 Python runtime import 初始化的 Logger（例如 `flash_ticket.worker`）其內部的 `logger.disabled` 屬性皆被靜默設為 `True`。這導致即使 Logger 宣告了 `INFO` 等級，`logger.isEnabledFor(INFO)` 依然回傳 `False`，所有 Worker 執行與全鏈路 Relay 日誌被徹底丟棄。
- **發現與修正方式**：
  在執行端到端全鏈路測試 `test_full_chain_http_to_outbox_to_worker_database` 時，發現 Outbox 狀態雖切換為 published，但日誌捕捉容器長度始終為 0。透過深入檢查 Logger 實例狀態發現 `disabled=True`。修正方式為：
  1. 在 `migrations/env.py` 明確傳入 `fileConfig(config.config_file_name, disable_existing_loggers=False)`。
  2. 在 `setup_logging()` 中為 `flash_ticket` 根命名空間明確設定日誌等級，杜絕現有及未來 Logger 被協力模組靜默停用。
