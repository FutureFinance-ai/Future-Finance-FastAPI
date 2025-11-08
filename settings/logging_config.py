from __future__ import annotations

import logging
from logging.config import dictConfig


def configure_logging(level: int = logging.INFO) -> None:
	dictConfig(
		{
			"version": 1,
			"disable_existing_loggers": False,
			"formatters": {
				"standard": {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"}
			},
			"handlers": {
				"console": {
					"class": "logging.StreamHandler",
					"formatter": "standard",
					"level": level,
				}
			},
			"loggers": {
				"": {"handlers": ["console"], "level": level},
				"uvicorn": {"handlers": ["console"], "level": level},
				"celery": {"handlers": ["console"], "level": level},
			},
		}
	)


