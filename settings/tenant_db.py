from __future__ import annotations

import asyncio
import hashlib
import pathlib
from typing import Optional

from surrealdb import AsyncSurreal

from .config import settings


TENANT_DB_NAME = "app"


def get_tenant_namespace(tenant_id: str) -> str:
	"""
	Compute the namespace name for a tenant.
	"""
	return f"tenant_{tenant_id}"


def stable_id_from_key(key: str) -> str:
	"""
	Create a stable record id suffix from an idempotency key.
	"""
	return hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]


async def new_db_connection() -> AsyncSurreal:
	"""
	Create a new AsyncSurreal connection using service credentials.
	Callers must close the connection when done.
	"""
	db = AsyncSurreal(settings.SURREALDB_URL)
	await db.signin(
		{
			"username": settings.SURREALDB_USER,
			"password": settings.SURREALDB_PASS,
		}
	)
	return db


async def get_db_for_tenant(tenant_id: str) -> AsyncSurreal:
	"""
	Return a new DB connection scoped to the tenant's namespace and `TENANT_DB_NAME`.
	"""
	db = await new_db_connection()
	await db.use(get_tenant_namespace(tenant_id), TENANT_DB_NAME)
	return db


async def ensure_ai_schema_for_tenant(tenant_id: str) -> None:
	"""
	Idempotently apply the AI schema for a given tenant namespace.
	"""
	db = await get_db_for_tenant(tenant_id)
	try:
		base_schema_path = pathlib.Path(__file__).resolve().parents[1] / "settings" / "surreal" / "schema.surql"
		ai_schema_path = pathlib.Path(__file__).resolve().parents[1] / "settings" / "surreal" / "ai_schema.surql"
		tenant_schema_path = pathlib.Path(__file__).resolve().parents[1] / "settings" / "surreal" / "tenant_schema.surql"
		# Apply existing base schema if present (non-fatal if absent)
		try:
			with open(base_schema_path, "r", encoding="utf-8") as f:
				await db.query(f.read())
		except FileNotFoundError:
			pass
		# Ensure user_scope exists in tenant namespace and any tenant-local declarations
		try:
			with open(tenant_schema_path, "r", encoding="utf-8") as f:
				await db.query(f.read())
		except FileNotFoundError:
			pass
		# Apply AI schema
		with open(ai_schema_path, "r", encoding="utf-8") as f:
			await db.query(f.read())
	finally:
		await db.close()

async def get_db_for_namespace(namespace: str) -> AsyncSurreal:
	"""
	Return a new DB connection scoped to the provided namespace and `TENANT_DB_NAME`.
	"""
	db = await new_db_connection()
	await db.use(namespace, TENANT_DB_NAME)
	return db


async def mint_scope_token(tenant_id: str, user_id: str) -> str:
	"""
	Mint a scoped SurrealDB token for the `app` scope embedding { tenant, user } claims.
	Use this token with `db.authenticate(token)` for defense-in-depth permissions.
	"""
	db = await new_db_connection()
	try:
		ns = get_tenant_namespace(tenant_id)
		await db.use(ns, TENANT_DB_NAME)
		token = await db.signin({"NS": ns, "DB": TENANT_DB_NAME, "SC": "app", "tenant": tenant_id, "user": user_id})
		return token  # type: ignore[return-value]
	finally:
		await db.close()


async def with_scoped_auth(db: AsyncSurreal, token: str) -> None:
	"""
	Attach a previously minted scope token to an existing DB connection.
	"""
	await db.authenticate(token)


