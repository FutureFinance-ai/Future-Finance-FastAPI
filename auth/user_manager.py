from fastapi import Depends, Request
from fastapi_users import BaseUserManager
from typing import Optional

from auth.db import get_user_db
from .config import settings

from .models import User
from users.user_repo import SurrealUserDatabase


class UserManager(BaseUserManager[User, str]):
    reset_password_token_secret = settings.ENV_RESET_PASSWORD_TOKEN_SECRET
    verification_token_secret = settings.ENV_VERIFICATION_TOKEN_SECRET

    async def on_after_register(self, user: User, request: Optional[Request] = None):
        print(f"User {user.email} has registered. Triggering email verification.")  # at least log something


async def get_user_manager(user_db: SurrealUserDatabase = Depends(get_user_db)) -> UserManager:
    yield UserManager(user_db)
