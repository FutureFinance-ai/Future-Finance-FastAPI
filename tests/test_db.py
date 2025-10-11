import pytest
import pytest_asyncio
from surrealdb import Surreal

from auth.db import get_db


@pytest_asyncio.fixture
async def db():
    # Ensure we start with a fresh connection each test
    db = await get_db()
    yield db
    # Cleanup: drop test tables/namespace if needed
    await db.query("REMOVE TABLE person")


@pytest.mark.asyncio
async def test_db_connection(db: Surreal):
    assert db is not None

    # create record
    record = await db.create("person", {"name": "Alice", "age": 30})
    assert record["name"] == "Alice"

    # read record
    people = await db.select("person")
    assert len(people) == 1
    assert people[0]["age"] == 30



from users.user_repo import SurrealUserDatabase
from auth.db import get_db


@pytest_asyncio.fixture
async def user_db():
    db = await get_db()
    user_db = SurrealUserDatabase(db, "users")
    yield user_db
    await db.query("REMOVE TABLE users ")


@pytest.mark.asyncio
async def test_create_and_get_user(user_db: SurrealUserDatabase):
    user_data = {"email": "test@example.com", "hashed_password": "secret"}
    created = await user_db.create(user_data)

    assert created.email == "test@example.com"

    fetched = await user_db.get(created.id)
    assert fetched is not None
    assert fetched.email == "test@example.com"


@pytest.mark.asyncio
async def test_get_by_email(user_db: SurrealUserDatabase):
    user_data = {"email": "findme@example.com", "hashed_password": "secret"}
    created = await user_db.create(user_data)

    found = await user_db.get_by_email("findme@example.com")
    assert found is not None
    assert found.id == created.id
