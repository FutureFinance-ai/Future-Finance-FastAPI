from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
        
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # Database settings (required; provided via .env)
    # SurrealDB (legacy; still used during migration)
    SURREALDB_URL: str = "ws://localhost:8000/rpc"
    SURREALDB_NS: str = "futurefinance"
    SURREALDB_DB: str = "main"
    SURREALDB_USER: str = "nameofapp"
    SURREALDB_PASS: str = "password123"
    # Postgres (primary OLTP target)
    POSTGRES_DSN: str = "postgresql+asyncpg://app_user:app_password@localhost:5432/futurefinance"
    # Optional: pgbouncer DSN if used; fallback to POSTGRES_DSN when unset
    PGBOUNCER_DSN: str | None = None

    # AWS settings
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: str = "your-access-key-id"
    AWS_SECRET_ACCESS_KEY: str = "your-secret-access-key"
    AWS_S3_BUCKET_NAME: str = "your-bucket-name"

    # Vector store (Qdrant)
    QDRANT_URL: str = "http://localhost:6333"
    QDRANT_API_KEY: str | None = None

    # Queues
    REDIS_URL: str | None = None

    # Auth secrets
    ENV_SECRET: str
    ENV_RESET_PASSWORD_TOKEN_SECRET: str
    ENV_VERIFICATION_TOKEN_SECRET: str

    # Feature flags
    ENABLE_BUDGETS: bool = False

    # SMTP / Alerts
    SMTP_HOST: str | None = None
    SMTP_PORT: int | None = None
    SMTP_USER: str | None = None
    SMTP_PASS: str | None = None
    ALERTS_FROM_EMAIL: str | None = None

    # LLM providers
    OPENAI_API_KEY: str | None = None

settings = Settings() 