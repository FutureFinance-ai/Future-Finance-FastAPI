import pytest
import pytest_asyncio
from fastapi import Request
from auth.user_manager import UserManager

from users.user_repo import SurrealUserDatabase


@pytest_asyncio.fixture
async def user_manager(fake_db):
    user_db = SurrealUserDatabase(fake_db, "users")
    yield UserManager(user_db)
    await fake_db.query("REMOVE TABLE users")


# @pytest.mark.asyncio
# async def test_user_register_and_retrieve(user_manager: UserManager):
#     user_data = {"email": "manager@example.com", "hashed_password": "secret", "is_verified": True, "is_active": True}
#     created = await user_manager.user_db.create(user_data)

#     assert created.email == "manager@example.com"

#     fetched = await user_manager.user_db.get(created.id)
#     assert fetched is not None
#     assert fetched.email == "manager@example.com"


@pytest.mark.asyncio
async def test_on_after_register(user_manager: UserManager):
    # Simulate user
    user_data = {"email": "hook@example.com", "hashed_password": "secret", "is_verified": True, "is_active": True}
    created = await user_manager.user_db.create(user_data)

    # Should not raise errors
    await user_manager.on_after_register(created, Request({"type": "http"}))

@pytest.mark.asyncio
async def test_get_by_email_found(user_manager: UserManager):
    user_data = {"email": "found@example.com", "hashed_password": "secret", "is_verified": True, "is_active": True}
    created = await user_manager.user_db.create(user_data)

    found = await user_manager.user_db.get_by_email("found@example.com")
    assert found is not None
    assert found.id == created.id
    assert found.email == created.email


@pytest.mark.asyncio
async def test_get_by_email_not_found(user_manager: UserManager):
    missing = await user_manager.user_db.get_by_email("missing@example.com")
    assert missing is None


@pytest.mark.asyncio
async def test_get_by_email_handles_exception(user_manager: UserManager, monkeypatch):
    async def raise_error(query, vars):
        raise RuntimeError("DB failure")

    # Monkeypatch the underlying db.query to raise an exception
    monkeypatch.setattr(user_manager.user_db.db, "query", raise_error)

    result = await user_manager.user_db.get_by_email("error@example.com")
    # Our implementation returns None on exception (and logs it)
    assert result is None

