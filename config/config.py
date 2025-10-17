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

    # Auth secrets
    ENV_SECRET: str
    ENV_RESET_PASSWORD_TOKEN_SECRET: str
    ENV_VERIFICATION_TOKEN_SECRET: str

settings = Settings() 