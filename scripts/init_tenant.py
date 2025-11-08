import asyncio
import sys

from settings.tenant_db import ensure_ai_schema_for_tenant


async def main(tenant_id: str) -> None:
	await ensure_ai_schema_for_tenant(tenant_id)
	print(f"Applied AI schema for tenant: {tenant_id}")


if __name__ == "__main__":
	if len(sys.argv) < 2:
		print("Usage: python -m scripts.init_tenant <tenant_id>")
		sys.exit(1)
	asyncio.run(main(sys.argv[1]))


