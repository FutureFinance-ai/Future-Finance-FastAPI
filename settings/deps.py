from __future__ import annotations

from fastapi import Depends, Header, HTTPException, status
from surrealdb import AsyncSurreal

from auth.auth import get_current_active_user
from auth.models import User
from .tenant_db import (
	get_db_for_tenant,
	ensure_ai_schema_for_tenant,
	mint_scope_token,
	with_scoped_auth,
	new_db_connection,
)


async def get_tenant_id(x_tenant_id: str | None = Header(default=None)) -> str:
	"""
	Resolve the tenant id from the request.
	For now, require `X-Tenant-ID` header.
	"""
	if not x_tenant_id:
		raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing X-Tenant-ID header")
	return x_tenant_id


async def get_scoped_tenant_db(
	tenant_id: str = Depends(get_tenant_id),
	user: User = Depends(get_current_active_user),
) -> AsyncSurreal:
	"""
	Provide a tenant-scoped SurrealDB client authenticated with an app scope token
	containing { tenant, user } claims. Ensures AI schema is applied for the tenant.
	Callers must not close the connection; the dependency manages lifecycle.
	"""
	await ensure_ai_schema_for_tenant(tenant_id)
	db = await get_db_for_tenant(tenant_id)
	token = await mint_scope_token(tenant_id, user.id)
	await with_scoped_auth(db, token)
	try:
		yield db
	finally:
		await db.close()


async def get_db_from_surreal_bearer(authorization: str = Header(..., alias="Authorization")) -> AsyncSurreal:
	"""
	Authenticate directly to SurrealDB using a tenant-scoped Surreal JWT provided by the client.
	Header: Authorization: Bearer <surreal_jwt>
	"""
	if not authorization or not authorization.lower().startswith("bearer "):
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header")
	token = authorization.split(" ", 1)[1].strip()
	db = await new_db_connection()
	try:
		await db.authenticate(token)
		yield db
	finally:
		await db.close()


