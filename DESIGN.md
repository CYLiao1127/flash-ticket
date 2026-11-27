# Flash-Ticket 系統設計文件 (DESIGN.md)

> **文件狀態**：本文件於 Commit 23 先行建立「大數據串流報表匯出（512MB / 50 萬筆）」設計章節；完整系統架構圖（Mermaid）與 Q1–Q6 設計問答（含 Commit 永久連結）將於最終 Commit 24 補齊。

---

## 大數據串流 Excel 報表匯出設計（Commit 23）

### 1. API 規格

- **端點**：`GET /api/v1/reports/exports`
- **驗證與授權（RBAC）**：需攜帶有效 `Bearer` JWT（經 `get_current_active_user` 驗證 `token_version` 與 `is_active`）
  - **`user`（一般使用者）**：僅可匯出**自己建立**的活動；若指定他人建立的 `event_id`，回傳 `403 Forbidden`。
  - **`admin` / `viewer`**：可匯出全部活動或指定任意單一 `event_id`。
- **查詢參數**：

| 參數 | 型別 | 預設值 | 說明 |
| :--- | :--- | :--- | :--- |
| `event_id` | `int \| null` | `null` | 指定單一活動 ID；未指定時依角色範圍匯出（User 僅自己活動、Admin/Viewer 全部活動） |
| `date_from` | `datetime \| null` | `null` | 篩選訂單建立時間起點（ISO 8601，含時區或視為 UTC） |
| `date_to` | `datetime \| null` | `null` | 篩選訂單建立時間終點（ISO 8601，含時區或視為 UTC） |
| `batch_size` | `int` (`100`–`10000`) | `1000` | PostgreSQL Server-side Cursor 單次 fetch 批次大小（可配置，對應 `DEFAULT_BATCH_SIZE = 1000`） |

- **回應標頭**：
  - `Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
  - `Content-Disposition: attachment; filename="orders_report_<scope>_<YYYYMMDD_HHMMSS>.xlsx"`
  - 採 64KB chunked `StreamingResponse` 串流輸出，傳輸結束或連線中斷後於 `finally` 刪除暫存檔。

---

### 2. 工作表結構、時區與數值格式

實作位於 `app/reports/service.py` 與 `app/reports/router.py`：

1. **`Summary` 工作表（SQL 端聚合）**：
   - 於資料庫端以 `COUNT(DISTINCT o.id)`、`COUNT(oi.id)`、`SUM(...)` 計算整體 KPI（總訂單數、總票券數、已付款訂單數、票款總額、手續費總額、總營收），不在 Python 記憶體中累積明細。
   - 透過 `GROUP BY e.id, e.title, s.zone_code, z.name` 產生「各活動／區域彙總」表，涵蓋票券數、已付款票數與區域營收。
2. **`Details` 工作表（常數記憶體串流）**：
   - 以 `order_items JOIN orders JOIN events JOIN users JOIN seats` 讀取完整明細欄位（`order_id`, `item_id`, `event_id`, `event_title`, `user_email`, `zone_code`, `row_label`, `seat_number`, `seat_price`, `order_ticket_amount`, `service_fee`, `order_total_amount`, `order_status`, `item_status`, `created_at`, `paid_at`）。
   - **時區處理**：所有時間戳統一轉換為 `Asia/Taipei`（UTC+8），並於表頭明確標註 `(Asia/Taipei)`。
   - **金額型別**：所有金額欄位皆透過 `ws.write_number(..., float(val), money_fmt)` 寫入為 Excel 數值型別（XML `t="n"`）搭配 `#,##0.00` 格式，避免字串儲存導致 Excel 無法加總。

---

### 3. 512MB 記憶體限制架構設計與實測

為避免 `500,000` 筆資料在 512MB 容器內發生 OOM，系統採用三層常數記憶體設計：

1. **PostgreSQL 伺服器端具名游標（Server-Side Named Cursor）**：
   - 使用 `psycopg2` 具名游標 `conn.cursor(name="report_details_server_cursor")` 並設定 `cur.itersize = batch_size`（預設 `1000`），資料庫每次僅傳送一個 batch，避免客戶端一次載入 50 萬筆 tuple。
2. **磁碟暫存檔 + XlsxWriter `constant_memory=True`**：
   - **關鍵避坑**：XlsxWriter 文件明確指出 `in_memory=True`（或寫入 `BytesIO`）會覆蓋 `constant_memory=True` 並將整個工作簿留在 RAM 中。
   - 因此本實作使用 `tempfile.mkstemp(prefix="flash_ticket_report_", suffix=".xlsx")` 搭配 `xlsxwriter.Workbook(tmp_path, {"constant_memory": True, "strings_to_urls": False})`，每寫入一列即刷寫至磁碟 XML 暫存檔，Python 行程記憶體維持常數。
3. **非阻塞執行與暫存檔回收**：
   - 同步報表產生透過 `asyncio.to_thread(_generate_excel_sync, ...)` 執行，避免阻塞 FastAPI Event Loop。
   - `stream_and_cleanup_file` 以 64KB chunk 串流回傳，並在 `finally:` 區塊刪除暫存檔以防容器 `/tmp` 磁碟膨脹。

#### 實測結果（512 MiB 限制的 `api-1` 容器，`memory.max = 536,870,912 bytes`，詳見 `docs/evidence/q9_memory_profile.txt`）

| 測試模式 | 匯出筆數驗證 | `batch_size` | 耗時 | `.xlsx` 大小 | 記憶體峰值 | OOM 次數 |
| :--- | :--- | ---: | ---: | ---: | :--- | ---: |
| In-Process (`benchmark_report_export.py`) | 500,000 列 | 1,000 | 38.95 s | 28.74 MB | 行程 `VmHWM` **81.28 MiB** (+2.72 MiB) | `0` |
| 端對端 HTTP (`GET /api/v1/reports/exports`) | `X-Report-Rows: 500000` + `sheet2.xml` 實測 **500,000** 資料列 | 1,000 | 40.90 s | 28.83 MB | 容器 `cgroup anon` 峰值 **95.68 MiB** (+10.56 MiB，含 uvicorn + client) | `0` |

### 4. 部署注意事項

- **容器暫存空間**：匯出 50 萬筆時 `/tmp` 需保留約 150–250 MB 可用磁碟空間（供 XlsxWriter 暫存 XML 與最終 `.xlsx` 壓縮檔，完成後自動清理）。
- **反向代理逾時**：Nginx 針對 `/api/v1/reports/exports` 建議維持 `proxy_read_timeout >= 120s`（本專案 Nginx 與 Uvicorn 可穩定於 ~48 秒完成 50 萬筆串流）。
- **批次參數調校**：預設 `batch_size=1000` 兼顧記憶體與網路往返；若資料庫延遲較高可調至 `2000`–`5000`，記憶體增量仍維持在數 MB 等級。
