from __future__ import annotations

from fastapi_users import FastAPIUsers
from fastapi_users.authentication import AuthenticationBackend, BearerTransport, JWTStrategy
from fastapi import Depends

from .models import User
from .user_manager import get_user_manager
from .config import settings


bearer_transport = BearerTransport(tokenUrl="/auth/jwt/login")


def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(secret=settings.ENV_SECRET, lifetime_seconds=60 * 60 * 24)


auth_backend = AuthenticationBackend(
    name="jwt",
    transport=bearer_transport,
    get_strategy=get_jwt_strategy,
)


fastapi_users = FastAPIUsers[User, str](
    get_user_manager,
    [auth_backend],
)


# Dependency to require an authenticated, active user and return the user's ID
_current_active_user = fastapi_users.current_user(active=True)

async def get_current_user(user: User = Depends(_current_active_user)) -> str:
    return user.id

# Expose full user dependencies for reuse
_current_verified_user = fastapi_users.current_user(active=True, verified=True)

async def get_current_active_user(user: User = Depends(_current_active_user)) -> User:
    return user

async def get_current_verified_user(user: User = Depends(_current_verified_user)) -> User:
    return user

