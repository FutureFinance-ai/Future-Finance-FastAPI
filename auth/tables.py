from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase
from fastapi_users_db_sqlalchemy import SQLAlchemyBaseUserTableUUID
from db.models import Base as AppBase


class UserTable(SQLAlchemyBaseUserTableUUID, AppBase):
    __tablename__ = "users"


