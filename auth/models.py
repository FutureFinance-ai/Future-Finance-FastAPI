from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, EmailStr


class User(BaseModel):
    id: str
    email: EmailStr
    hashed_password: str
    is_active: bool = True
    is_superuser: bool = False
    is_verified: bool = False
    created_at: str | None = None
    updated_at: str | None = None
    # is_superuser: bool = False



