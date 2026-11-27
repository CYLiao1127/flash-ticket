import logging
import sys
import time
from contextvars import ContextVar
from typing import Optional
from app.config import settings

# 全域非同步安全的 Request ID 與認證 User ID Holder ContextVar
# 使用可變 dict 作為 holder，確保 Starlette BaseHTTPMiddleware 子協程內寫入之 user_id 可被父協程讀取
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")
request_user_holder_ctx: ContextVar[Optional[dict]] = ContextVar(
    "request_user_holder", default=None
)


def get_request_id() -> str:
    """獲取當前協程上下文之 Request ID"""
    return request_id_ctx.get()


def set_request_user_id(user_id: Optional[int]) -> None:
    """於當前請求上下文紀錄已認證之 User ID"""
    holder = request_user_holder_ctx.get()
    if holder is not None:
        holder["user_id"] = user_id


def get_request_user_id() -> Optional[int]:
    """獲取當前協程上下文已認證之 User ID"""
    holder = request_user_holder_ctx.get()
    return holder.get("user_id") if holder is not None else None


class TracingFilter(logging.Filter):
    """日誌過濾器：自動為每筆日誌附加 instance_id 與 request_id"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        record.instance_id = settings.INSTANCE_ID
        return True


def setup_logging(log_level: Optional[str] = None) -> None:
    """設定全域日誌格式與處理器"""
    level = (log_level or settings.LOG_LEVEL).upper()

    log_format = (
        "%(asctime)s [%(levelname)s] [%(instance_id)s] [%(request_id)s] "
        "[%(name)s:%(lineno)d] %(message)s"
    )

    formatter = logging.Formatter(fmt=log_format, datefmt="%Y-%m-%d %H:%M:%S")

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.addFilter(TracingFilter())

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # 確保 flash_ticket 命名空間之 Logger 具備明確的 Log Level
    app_logger = logging.getLogger("flash_ticket")
    app_logger.setLevel(level)

    # 清理舊有的處理器避免重複輸出
    root_logger.handlers = [handler]

    # 同步配置 uvicorn 與 fastapi 記錄器
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        l = logging.getLogger(logger_name)
        l.handlers = [handler]
        l.propagate = False
