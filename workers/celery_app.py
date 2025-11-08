from __future__ import annotations

import os
from celery import Celery

BROKER_URL = os.getenv("CELERY_BROKER_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", BROKER_URL)

app = Celery("futurefinance", broker=BROKER_URL, backend=RESULT_BACKEND, include=["workers.tasks.categorize", "workers.tasks.ordering", "workers.tasks.subscriptions", "workers.tasks.insights", "workers.tasks.forecast"])

# Sensible defaults
app.conf.update(
	task_serializer="json",
	accept_content=["json"],
	result_serializer="json",
	timezone="UTC",
	enable_utc=True,
)

# Optional: Celery beat schedules (example nightly jobs; tenant iteration should be handled externally)
app.conf.beat_schedule = {
	"nightly-generate-insights": {
		"task": "generate_insights",
		"schedule": 60 * 60 * 24,  # every 24h
		"args": ("default", "system"),  # placeholder; production should enqueue per-tenant,user
	},
}


