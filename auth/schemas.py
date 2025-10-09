from __future__ import annotations

from typing import Optional

from fastapi_users import schemas


class UserRead(schemas.BaseUser):
    pass


class UserCreate(schemas.BaseUserCreate):
    pass


class UserUpdate(schemas.BaseUserUpdate):
    pass



