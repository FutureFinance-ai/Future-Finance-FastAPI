from __future__ import annotations

from arq.connections import RedisSettings
from arq import cron

from settings.config import settings
from workers.categorization_worker import categorize_new_transactions
from workers.insight_worker import generate_user_insights
from workers.forecasting_worker import forecast_account_balance


class WorkerSettings:
    functions = [
        categorize_new_transactions,
        generate_user_insights,
        forecast_account_balance,
    ]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL or "redis://localhost:6379")
    cron_jobs = [
        cron(generate_user_insights, hour=1, minute=0),     # Nightly insights at 1:00 AM
        cron(forecast_account_balance, hour=2, minute=0),   # Nightly forecasts at 2:00 AM
    ]


