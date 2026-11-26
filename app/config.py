from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
  model_config = SettingsConfigDict(
      env_file=".env", env_file_encoding="utf-8", extra="ignore"
  )

  # Environment
  ENVIRONMENT: str = "development"
  LOG_LEVEL: str = "INFO"
  INSTANCE_ID: str = "api-default"

  # Application & Server
  APP_HOST: str = "0.0.0.0"
  APP_PORT: int = 8000
  SECRET_KEY: str = "dev-super-secret-key-change-in-production-min-32-chars"
  JWT_ALGORITHM: str = "HS256"
  ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

  # Database
  POSTGRES_USER: str = "flash_user"
  POSTGRES_PASSWORD: str = "flash_password"
  POSTGRES_DB: str = "flash_ticket"
  POSTGRES_HOST: str = "postgres"
  POSTGRES_PORT: int = 5432
  DATABASE_URL: str = "postgresql+asyncpg://flash_user:flash_password@postgres:5432/flash_ticket"
  SYNC_DATABASE_URL: str = (
      "postgresql://flash_user:flash_password@postgres:5432/flash_ticket"
  )

  # Redis
  REDIS_HOST: str = "redis"
  REDIS_PORT: int = 6379
  REDIS_URL: str = "redis://redis:6379/0"

  # Payment
  PAYMENT_WEBHOOK_SECRET: str = "mock-payment-signature-secret-key-32chars"
  PAYMENT_TIMEOUT_SECONDS: int = 600

  # Alerts & Metrics
  ALERT_ERROR_RATE_THRESHOLD: float = 0.05
  ALERT_P99_LATENCY_MS: int = 2000
  ALERT_OUTBOX_LAG_THRESHOLD: int = 1000

  # Timezone
  TIMEZONE: str = "Asia/Taipei"


settings = Settings()
