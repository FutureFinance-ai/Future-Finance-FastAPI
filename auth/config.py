from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
        
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    SURREALDB_URL: str
    SURREALDB_NS: str
    SURREALDB_DB: str
    SURREALDB_USER: str
    SURREALDB_PASS: str

    # Auth secrets (NO DEFAULTS)
    ENV_SECRET: str
    ENV_RESET_PASSWORD_TOKEN_SECRET: str
    ENV_VERIFICATION_TOKEN_SECRET: str

    # --- OPTIONAL SETTINGS (With safe defaults or None) ---

    # Feature flags
    ENABLE_BUDGETS: bool = False

    # SMTP / Alerts (Optional, but validated if provided)
    # SMTP_HOST: str | None = None
    # # Add validation: port must be between 1 and 65535
    # SMTP_PORT: int | None = Field(default=None, gt=0, le=65535)
    # SMTP_USER: str | None = None
    # SMTP_PASS: str | None = None
    # ALERTS_FROM_EMAIL: str | None = None

    # AWS S3 / Redis worker (Optional)
    AWS_REGION: str | None = None
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_BUCKET_NAME: str | None = None
    REDIS_URL: str | None = None

settings = Settings() 