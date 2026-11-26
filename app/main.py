from fastapi import FastAPI

app = FastAPI(
    title="Flash Ticket API",
    description="高併發活動搶票與即時監控平台 API",
    version="1.0.0",
)


@app.get("/health", tags=["Health"])
async def health_check():
  """服務健康檢查探針"""
  return {"status": "ok"}
