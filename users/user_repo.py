from typing import Optional, Union, Any, Dict
from datetime import datetime, timezone
import logging

from fastapi import HTTPException
from fastapi_users.db import BaseUserDatabase
from surrealdb import AsyncSurreal
from auth.models import User


logger = logging.getLogger(__name__)

class SurrealUserDatabase(BaseUserDatabase[User, str]):
    def __init__(self, db: AsyncSurreal, collection: str = "users") -> None:
        self.db = db
        self.collection = collection

    async def get(self, id: Union[str, int]) -> Optional[User]:
        user_id = str(id).split(":")[1]
        try:
            query = "SELECT * FROM type::thing('users', $user_id)"
            vars = {
                "user_id": user_id
            }

            result = await self.db.query(query, vars)
            record = result[0]
            if record:
                return User(**self._normalize_record(record))
            return None
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Error querying user by id: {exc}")

    async def get_by_email(self, email: str) -> Optional[User]:
        try:
            query = f"SELECT * FROM {self.collection} WHERE email = $email"
            vars = {"email": email}
            results = await self.db.query(query, vars)

            if results and results[0]:
                user = User(**self._normalize_record(results[0]))
                return user
            return None
        except Exception as exc:
            logger.exception("Error querying user by email from collection '%s': %s", self.collection, exc)
            raise HTTPException(status_code=500, detail="Error querying user by email")
            

    async def create(self, create_dict: dict) -> User:
        now_iso = datetime.now(timezone.utc).isoformat()
        # Ensure required flags and timestamps exist on create
        payload = {
            **create_dict,
            "is_active": create_dict.get("is_active", True),
            "is_superuser": create_dict.get("is_superuser", False),
            "is_verified": create_dict.get("is_verified", False),
            "created_at": create_dict.get("created_at", now_iso),
            "updated_at": create_dict.get("updated_at", now_iso),
        }
        record = await self.db.create(self.collection, payload)
        return User(**self._normalize_record(record))

    async def update(self, user: User, update_dict: dict) -> User:
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            **update_dict,
            "updated_at": now_iso,
        }
        record = await self.db.update(f"{self.collection}:{user.id}", payload)
        return User(**self._normalize_record(record))

    async def delete(self, user: User) -> None:
        await self.db.delete(f"{self.collection}:{user.id}")

    def _normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        if "id" in record:
            record = {**record, "id": str(record["id"]) }
        return record
