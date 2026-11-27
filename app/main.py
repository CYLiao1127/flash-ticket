import time
import uuid
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from app.core.logging import setup_logging, request_id_ctx
from app.core.redis import close_redis
from app.auth.router import router as auth_router, admin_users_router
from app.events.router import router as events_router
from app.auth.dependencies import public_endpoint

logger = logging.getLogger("flash_ticket.api")
logger.setLevel(logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 服務啟動初始化
    setup_logging()
    logger.info("Flash Ticket API service initializing...")
    yield
    logger.info("Flash Ticket API service shutting down...")
    await close_redis()


app = FastAPI(
    title="Flash Ticket API",
    description="高併發活動搶票與即時監控平台 API",
    version="1.0.0",
    lifespan=lifespan,
)

# 註冊 API 路由
app.include_router(auth_router)
app.include_router(admin_users_router)
app.include_router(events_router)


@app.middleware("http")
async def request_tracing_middleware(request: Request, call_next):
    """
    全鏈路請求追蹤中介軟體 (X-Request-ID Middleware)
    1. 若 Client 帶有 X-Request-ID 則繼承，否則由系統生成 UUID4
    2. 綁定至協程安全的 ContextVar 及 request.state
    3. 計算請求處理延遲 (Latency)
    4. 於回應標頭附加 X-Request-ID 回傳 Client
    """
    client_request_id = request.headers.get("X-Request-ID")
    req_id = client_request_id.strip() if client_request_id and client_request_id.strip() else uuid.uuid4().hex

    token = request_id_ctx.set(req_id)
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
        logger.info(
            f"{request.method} {request.url.path} - status={status_code} "
            f"latency={latency_ms:.2f}ms client_ip={client_ip}"
        )
        request_id_ctx.reset(token)


@app.get("/health", tags=["Health"])
@public_endpoint("Container health check probe")
async def health_check():
    """服務健康檢查探針"""
    return {"status": "ok"}
