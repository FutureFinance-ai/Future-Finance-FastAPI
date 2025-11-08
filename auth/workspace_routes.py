from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Any

from auth.auth import get_current_active_user
from auth.models import User
from surrealdb import AsyncSurreal
from auth.db import get_db
from settings.tenant_db import new_db_connection, TENANT_DB_NAME


router = APIRouter(prefix="/auth", tags=["auth-workspace"])


@router.get("/my-tenants")
async def my_tenants(user: User = Depends(get_current_active_user), db: AsyncSurreal = Depends(get_db)) -> list[dict[str, Any]]:
	"""
	Return the list of tenants (workspaces) the current global user belongs to.
	"""
	res = await db.query("SELECT tenant FROM membership WHERE user = $u FETCH tenant", {"u": user.id})
	rows = res[0].get("result", []) if res else []
	tenants: list[dict[str, Any]] = []
	for row in rows:
		tenant = row.get("tenant")
		if isinstance(tenant, dict):
			tenants.append({"id": tenant.get("id"), "name": tenant.get("name"), "slug": tenant.get("slug")})
	return tenants


class SelectTenantIn(BaseModel):
	tenant_slug: str


@router.post("/select-tenant")
async def select_tenant(payload: SelectTenantIn, user: User = Depends(get_current_active_user), db: AsyncSurreal = Depends(get_db)) -> dict[str, str]:
	"""
	Exchange the global auth (FastAPI Users) identity for a tenant-scoped SurrealDB JWT.
	"""
	# Validate tenant exists
	tq = await db.query("SELECT * FROM tenant WHERE slug = $slug LIMIT 1", {"slug": payload.tenant_slug})
	trows = tq[0].get("result", []) if tq else []
	if not trows:
		raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found")
	tenant_id = trows[0]["id"]
	# Validate membership and fetch role
	mq = await db.query("SELECT role FROM membership WHERE user = $u AND tenant = $t LIMIT 1", {"u": user.id, "t": tenant_id})
	mrows = mq[0].get("result", []) if mq else []
	if not mrows:
		raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not a member of this tenant")
	role = mrows[0].get("role", "member")
	# Sign into tenant namespace to mint user_scope token
	tenant_ns = payload.tenant_slug
	conn = await new_db_connection()
	try:
		await conn.use(tenant_ns, TENANT_DB_NAME)
		token = await conn.signin({"NS": tenant_ns, "DB": TENANT_DB_NAME, "SC": "user_scope", "tenant": tenant_ns, "user": user.id, "role": role})
		return {"surreal_token": token, "namespace": tenant_ns}
	finally:
		await conn.close()


