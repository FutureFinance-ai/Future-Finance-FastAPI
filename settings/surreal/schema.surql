-- USERS: aligns with auth/models.py and fastapi-users expectations
-- Single service credential in app => enforce row access in API layer, not DB
DEFINE TABLE users SCHEMAFULL PERMISSIONS FULL;

DEFINE FIELD email           ON users TYPE string ASSERT is::email($value);
DEFINE FIELD hashed_password ON users TYPE string;
DEFINE FIELD is_active       ON users TYPE bool DEFAULT true;
DEFINE FIELD is_superuser    ON users TYPE bool DEFAULT false;
DEFINE FIELD is_verified     ON users TYPE bool DEFAULT false;
DEFINE FIELD name            ON users TYPE option<string>;
DEFINE FIELD created_at      ON users TYPE datetime VALUE $before OR time::now();
DEFINE FIELD updated_at      ON users TYPE datetime VALUE time::now();

DEFINE INDEX users_email_unique ON users FIELDS email UNIQUE;

-- ACCOUNT: matches UploadRepo account_data (name, number, opening_minor, closing_minor, currency, s3_raw_url, owner)
-- PERMISSIONS FULL so the service account can operate; filter by owner in queries
DEFINE TABLE account SCHEMAFULL PERMISSIONS FULL;

DEFINE FIELD owner           ON account TYPE record<users>;
DEFINE FIELD name            ON account TYPE string;
DEFINE FIELD number          ON account TYPE string;
DEFINE FIELD account_type    ON account TYPE option<string> ASSERT $value IN ['checking','savings','credit','investment','wallet','other'];
DEFINE FIELD institution     ON account TYPE option<string>;
DEFINE FIELD statement_date  ON account TYPE option<datetime>;
DEFINE FIELD opening_balance   ON account TYPE int;
DEFINE FIELD closing_balance   ON account TYPE int;
DEFINE FIELD currency        ON account TYPE string DEFAULT 'NGN';
DEFINE FIELD s3_raw_url      ON account TYPE string ASSERT is::url($value);
DEFINE FIELD created_at      ON account TYPE datetime VALUE $before OR time::now();
DEFINE FIELD updated_at      ON account TYPE datetime VALUE time::now();

DEFINE INDEX account_owner_idx   ON account FIELDS owner;
DEFINE INDEX account_number_idx  ON account FIELDS number;

-- TRANSACTION: stores debit, credit, balance (minor units) and derived amount_minor
-- PERMISSIONS FULL; API layer must include account.owner = $user_id in all reads/updates
DEFINE TABLE transaction SCHEMAFULL PERMISSIONS FULL;

DEFINE FIELD account         ON transaction TYPE record<account>;
DEFINE FIELD trans_time      ON transaction TYPE option<datetime>;
DEFINE FIELD value_date      ON transaction TYPE option<datetime>;
DEFINE FIELD description     ON transaction TYPE string;
DEFINE FIELD amount_minor    ON transaction TYPE int;
DEFINE FIELD debit           ON transaction TYPE option<int>;
DEFINE FIELD credit          ON transaction TYPE option<int>;
DEFINE FIELD balance         ON transaction TYPE option<int>;
DEFINE FIELD category        ON transaction TYPE option<string>;
DEFINE FIELD counterparty    ON transaction TYPE option<string>;
DEFINE FIELD upload_id       ON transaction TYPE string;
DEFINE FIELD currency        ON transaction TYPE string DEFAULT 'NGN';
DEFINE FIELD created_at      ON transaction TYPE datetime VALUE $before OR time::now();

-- Indices for critical query paths
DEFINE INDEX txn_account_time_idx        ON transaction FIELDS account, trans_time;
DEFINE INDEX txn_account_value_date_idx  ON transaction FIELDS account, value_date;
DEFINE INDEX txn_account_category_idx    ON transaction FIELDS account, category;
DEFINE INDEX txn_upload_idx              ON transaction FIELDS upload_id;
DEFINE INDEX txn_description_idx         ON transaction FIELDS description BM25(HIGHLIGHTS);

-- Optional explicit graph edges (use with RELATE)
DEFINE TABLE owns SCHEMALESS PERMISSIONS NONE; -- users -> account
DEFINE TABLE has  SCHEMALESS PERMISSIONS NONE; -- account -> transaction

