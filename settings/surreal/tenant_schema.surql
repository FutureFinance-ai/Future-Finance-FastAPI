-- Tenants and memberships live in the global namespace (auth DB)
DEFINE TABLE tenant SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD name ON tenant TYPE string;
DEFINE FIELD slug ON tenant TYPE string;
DEFINE INDEX tenant_slug_unique ON tenant FIELDS slug UNIQUE;

DEFINE TABLE membership SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD user   ON membership TYPE record<users>;
DEFINE FIELD tenant ON membership TYPE record<tenant>;
DEFINE FIELD role   ON membership TYPE string ASSERT $value IN ['owner','admin','member','viewer'];
DEFINE INDEX membership_user_idx   ON membership FIELDS user;
DEFINE INDEX membership_tenant_idx ON membership FIELDS tenant;

-- Scope used for tenant-scoped user sessions (this scope should also exist in each tenant namespace)
DEFINE SCOPE user_scope SESSION 7d SIGNIN(RETURN { tenant: $tenant, user: $user, role: $role });


