import os
import sys

# Provide required auth secrets for tests if not already set
os.environ.setdefault("ENV_SECRET", "test-secret")
os.environ.setdefault("ENV_RESET_PASSWORD_TOKEN_SECRET", "test-reset-secret")
os.environ.setdefault("ENV_VERIFICATION_TOKEN_SECRET", "test-verify-secret")


def pytest_sessionstart(session):
    # Ensure project root is on sys.path so `auth` and `users` resolve
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


# --- Test utilities: Fake in-memory SurrealDB ---
import uuid
import asyncio
import pytest_asyncio


class FakeAsyncSurreal:
    def __init__(self) -> None:
        self._tables = {"users": {}}

    async def select(self, key: str):
        # Expecting format "users:<id>"
        if ":" in key:
            table, id_part = key.split(":", 1)
            rec = self._tables.get(table, {}).get(id_part)
            if rec is None:
                return None
            # Return a plain dict like the real client
            return {**rec}
        # Not used in current tests
        return None

    async def query(self, query: str, vars: dict | None = None):
        # Handle cleanup
        if query.strip().upper() == "REMOVE TABLE USERS":
            self._tables["users"] = {}
            return [{"result": []}]

        # Handle select by email
        if query.strip().upper().startswith("SELECT * FROM USERS WHERE EMAIL = $EMAIL"):
            email = vars.get("email") if vars else None
            users_table = self._tables["users"]
            for rec in users_table.values():
                if rec.get("email") == email:
                    return [{"result": [{**rec}]}]
            return [{"result": []}]

        # Default empty result for unrecognized queries used in tests
        return [{"result": []}]

    async def create(self, table: str, payload: dict):
        assert table == "users"
        new_id = str(uuid.uuid4())
        # Simulate SurrealDB id like "users:<uuid>"
        record = {**payload, "id": f"users:{new_id}"}
        self._tables["users"][new_id] = record
        return {**record}

    async def update(self, key: str, payload: dict):
        table, id_part = key.split(":", 1)
        current = self._tables.get(table, {}).get(id_part)
        if current is None:
            raise KeyError(key)
        updated = {**current, **payload}
        self._tables[table][id_part] = updated
        return {**updated}

    async def delete(self, key: str):
        table, id_part = key.split(":", 1)
        self._tables.get(table, {}).pop(id_part, None)


@pytest_asyncio.fixture
async def fake_db():
    # Provide a fresh fake DB per test function
    db = FakeAsyncSurreal()
    yield db


