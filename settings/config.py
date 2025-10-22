from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
        
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # Database settings (required; provided via .env)
    SURREALDB_URL: str = "ws://localhost:8000/rpc"
    SURREALDB_NS: str = "futurefinance"
    SURREALDB_DB: str = "main"
    SURREALDB_USER: str = "nameofapp"
    SURREALDB_PASS: str = "password123"

    # AWS settings
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: str = "your-access-key-id"
    AWS_SECRET_ACCESS_KEY: str = "your-secret-access-key"
    AWS_S3_BUCKET_NAME: str = "your-bucket-name"

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

settings = Settings() 