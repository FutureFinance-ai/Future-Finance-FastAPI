import asyncio
import sys
from typing import Optional

from auth.db import get_db, init_db, close_db
from settings.config import settings
from surrealdb import AsyncSurreal


async def backfill_default_memberships(default_slug: str = "default") -> None:
	await init_db()
	db: AsyncSurreal = await get_db()  # type: ignore
	try:
		# Ensure tenant exists
		tq = await db.query("SELECT * FROM tenant WHERE slug = $s LIMIT 1", {"s": default_slug})
		trows = tq[0].get("result", []) if tq else []
		if not trows:
			tenant = await db.create("tenant", {"name": "Default", "slug": default_slug})
			tenant_id = tenant["id"] if isinstance(tenant, dict) else tenant[0]["id"]
		else:
			tenant_id = trows[0]["id"]
		# Fetch all users
		uq = await db.query("SELECT id FROM users")
		users = uq[0].get("result", []) if uq else []
		for u in users:
			uid = u["id"]
			# Create membership if absent
			mq = await db.query("SELECT * FROM membership WHERE user = $u AND tenant = $t LIMIT 1", {"u": uid, "t": tenant_id})
			if not (mq and mq[0].get("result")):
				await db.create("membership", {"user": uid, "tenant": tenant_id, "role": "member"})
	finally:
		await close_db()


if __name__ == "__main__":
	slug = sys.argv[1] if len(sys.argv) > 1 else "default"
	asyncio.run(backfill_default_memberships(slug))


