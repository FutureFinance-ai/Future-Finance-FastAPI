from fastapi import Depends, Request
from fastapi_users import BaseUserManager
from typing import Optional
import logging

from auth.db import get_user_db
from .config import settings

from .models import User
from users.user_repo import SurrealUserDatabase

logger = logging.getLogger(__name__)


class UserManager(BaseUserManager[User, str]):
    reset_password_token_secret = settings.ENV_RESET_PASSWORD_TOKEN_SECRET
    verification_token_secret = settings.ENV_VERIFICATION_TOKEN_SECRET

    def parse_id(self, value: str) -> str:
        # Our IDs are stored as SurrealDB record ids like "users:<uuid>".
        # We store and compare them as strings, so return as-is.
        return value

    async def on_after_register(self, user: User, request: Optional[Request] = None):
        logger.info(f"User {user.email} has registered. Triggering email verification.")  # at least log something


async def get_user_manager(user_db: SurrealUserDatabase = Depends(get_user_db)) -> UserManager:
    yield UserManager(user_db)
