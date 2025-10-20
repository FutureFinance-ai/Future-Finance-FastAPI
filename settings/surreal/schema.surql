-- Namespaces and DB assumed configured elsewhere

DEFINE TABLE account SCHEMAFULL;
DEFINE FIELD name           ON account TYPE string ASSERT $value != '';
DEFINE FIELD number         ON account TYPE string ASSERT $value != '';
DEFINE FIELD opening_minor  ON account TYPE int;
DEFINE FIELD closing_minor  ON account TYPE int;
DEFINE FIELD currency       ON account TYPE string;
DEFINE FIELD s3_raw_url     ON account TYPE string;
DEFINE FIELD owner          ON account TYPE string;
DEFINE FIELD created_at     ON account TYPE datetime;

DEFINE INDEX idx_account_owner        ON account FIELDS owner;
DEFINE INDEX idx_account_number_owner ON account FIELDS number, owner UNIQUE;

DEFINE TABLE transaction SCHEMAFULL;
DEFINE FIELD account       ON transaction TYPE record(account) ASSERT $value != NONE;
DEFINE FIELD value_date    ON transaction TYPE datetime;
DEFINE FIELD trans_time    ON transaction TYPE datetime;
DEFINE FIELD description   ON transaction TYPE string;
DEFINE FIELD amount_minor  ON transaction TYPE int;
DEFINE FIELD balance_minor ON transaction TYPE int;
DEFINE FIELD created_at    ON transaction TYPE datetime;
DEFINE FIELD upload_id     ON transaction TYPE string;

DEFINE INDEX idx_txn_account_date   ON transaction FIELDS account, value_date;
DEFINE INDEX idx_txn_account        ON transaction FIELDS account;

DEFINE INDEX fts_txn_description ON transaction FIELDS description SEARCH ANALYZER english;

-- Optional rollup table for analytics
DEFINE TABLE txn_rollup SCHEMAFULL;
DEFINE FIELD account       ON txn_rollup TYPE record(account) ASSERT $value != NONE;
DEFINE FIELD period_start  ON txn_rollup TYPE datetime;  -- month/day boundary
DEFINE FIELD credits_minor ON txn_rollup TYPE int;
DEFINE FIELD debits_minor  ON txn_rollup TYPE int;
DEFINE FIELD net_minor     ON txn_rollup TYPE int;
DEFINE FIELD created_at    ON txn_rollup TYPE datetime;
DEFINE FIELD updated_at    ON txn_rollup TYPE datetime;

DEFINE INDEX idx_rollup_account_period ON txn_rollup FIELDS account, period_start UNIQUE;

