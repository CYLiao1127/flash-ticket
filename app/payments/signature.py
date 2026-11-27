"""HMAC-SHA256 Webhook 簽章生成與防重放驗證工具"""

import hashlib
import hmac
import time
from typing import Optional

from fastapi import HTTPException, status

from app.config import settings

WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS = 300  # 5 分鐘防重放視窗


def compute_webhook_signature(
    timestamp: str,
    raw_body: bytes,
    secret: Optional[str] = None,
) -> str:
    """以 HMAC-SHA256 計算簽章：HMAC(secret, '<timestamp>.<raw_body>')"""
    signing_key = (secret or settings.PAYMENT_WEBHOOK_SECRET).encode("utf-8")
    signed_payload = f"{timestamp}.".encode("utf-8") + raw_body
    return hmac.new(signing_key, signed_payload, hashlib.sha256).hexdigest()


def verify_webhook_signature(
    timestamp_header: Optional[str],
    signature_header: Optional[str],
    raw_body: bytes,
    now_ts: Optional[int] = None,
    secret: Optional[str] = None,
) -> int:
    """
    驗證金流 Webhook HMAC-SHA256 簽章與 5 分鐘防重放時間視窗。
    驗證失敗統一回傳 400 Bad Request。
    """
    if not timestamp_header or not signature_header:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing X-Webhook-Timestamp or X-Webhook-Signature header",
        )

    ts_str = timestamp_header.strip()
    sig_str = signature_header.strip()
    try:
        ts_val = int(ts_str)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid X-Webhook-Timestamp header; must be Unix epoch seconds",
        )

    current_ts = int(time.time()) if now_ts is None else int(now_ts)
    if abs(current_ts - ts_val) > WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook timestamp is outside the 5-minute replay tolerance window",
        )

    expected_sig = compute_webhook_signature(ts_str, raw_body, secret=secret)
    if not hmac.compare_digest(expected_sig, sig_str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid webhook HMAC-SHA256 signature",
        )

    return ts_val
