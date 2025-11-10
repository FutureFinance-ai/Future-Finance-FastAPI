from fastapi import Depends, Request
from fastapi_users import BaseUserManager
from typing import Optional
import logging

from .config import settings
from .schemas import UserRead
from .sqlalchemy_db import get_user_db
from fastapi_users_db_sqlalchemy import SQLAlchemyUserDatabase

logger = logging.getLogger(__name__)


class UserManager(BaseUserManager[UserRead, str]):
    reset_password_token_secret = settings.ENV_RESET_PASSWORD_TOKEN_SECRET
    verification_token_secret = settings.ENV_VERIFICATION_TOKEN_SECRET

    async def on_after_register(self, user: UserRead, request: Optional[Request] = None):
        logger.info(f"User {user.email} has registered.")


async def get_user_manager(user_db: SQLAlchemyUserDatabase = Depends(get_user_db)) -> UserManager:
    yield UserManager(user_db)
