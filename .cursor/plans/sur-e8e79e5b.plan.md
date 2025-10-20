<!-- e8e79e5b-1c67-4846-832c-34e78295af7c b7243b8e-639f-4e93-afae-3d6556f6749a -->
# SurrealDB Optimization Plan

## Goals

- **Fast ingestion** for 1k–10k transactions/upload with atomicity
- **Low-latency queries** for date ranges, full-text search, and aggregations
- **Clear ownership and schema guarantees** (typed fields, constraints)
- **Operational efficiency** (fewer round trips, reusable connections)

## 1) Schema and Types (SCHEMAFULL)

Create a Surreal schema file (e.g., `settings/surreal/schema.surql`) and load at startup:

```1:60:settings/surreal/schema.surql
-- Namespaces and DB assumed configured elsewhere

DEFINE TABLE account SCHEMAFULL;
DEFINE FIELD name           ON account TYPE string ASSERT $value != '';
DEFINE FIELD number         ON account TYPE string ASSERT $value != '';
DEFINE FIELD opening_minor  ON account TYPE int;          -- amount in minor units
DEFINE FIELD closing_minor  ON account TYPE int;
DEFINE FIELD currency       ON account TYPE string;       -- e.g. 'NGN'
DEFINE FIELD s3_raw_url     ON account TYPE string;
DEFINE FIELD owner          ON account TYPE string;       -- user id
DEFINE FIELD created_at     ON account TYPE datetime;

DEFINE INDEX idx_account_owner        ON account FIELDS owner;
DEFINE INDEX idx_account_number_owner ON account FIELDS number, owner UNIQUE; -- optional uniqueness

DEFINE TABLE transaction SCHEMAFULL;
DEFINE FIELD account       ON transaction TYPE record(account) ASSERT $value != NONE;
DEFINE FIELD value_date    ON transaction TYPE datetime;
DEFINE FIELD trans_time    ON transaction TYPE datetime; -- if available; else drop
DEFINE FIELD description   ON transaction TYPE string;
DEFINE FIELD amount_minor  ON transaction TYPE int;      -- signed: credit positive, debit negative
DEFINE FIELD balance_minor ON transaction TYPE int;      -- optional if statement supplies
DEFINE FIELD created_at    ON transaction TYPE datetime;

DEFINE INDEX idx_txn_account_date   ON transaction FIELDS account, value_date;
DEFINE INDEX idx_txn_account        ON transaction FIELDS account;

-- Full-text index for descriptions (uses built-in english analyzer; switch if needed)
DEFINE INDEX fts_txn_description ON transaction FIELDS description SEARCH ANALYZER english;
```

Why: strong typing, smaller ints for money, composite indexes for date-range scans, FTS for search, fewer floats.

## 2) Data Modeling Changes

- **Money as integers**: store cents/kobo in `*_minor` with a `currency` on `account`. Convert on ingest.
- **Single signed `amount_minor`** per transaction (credit positive, debit negative). Avoid `debit/credit` branches.
- **Dates as datetime**: parse to UTC on ingest; avoid strings.
- **Record link** `transaction.account -> account` for efficient lookups.

## 3) Bulk, Atomic Ingestion (one round trip)

Replace per-row creates with a single multi-statement query. Example shape called once:

```1:80:upload_service/upload_repo.py
async def save_user_upload(self, user_id, account_header, transactions, s3_url) -> str:
    account = {
        'name': account_header.account_name,
        'number': account_header.account_number,
        'opening_minor': opening_minor,
        'closing_minor': closing_minor,
        'currency': account_header.currency,
        's3_raw_url': s3_url,
        'owner': user_id,
        'created_at': time_now,
    }
    txns = [
        {
            'value_date': txn.value_date_utc,
            'trans_time': txn.trans_time_utc,
            'description': txn.description,
            'amount_minor': txn.amount_minor,    # signed
            'balance_minor': txn.balance_minor,
            'created_at': time_now,
        }
        for txn in transactions
    ]

    query = """
    BEGIN TRANSACTION;
    LET $acc = (CREATE account CONTENT $account);
    -- attach account id to each row server-side and insert in one go
    LET $rows = array::map($txns, function($t) { $t.account = type::thing($acc.id); return $t; });
    INSERT INTO transaction $rows;
    COMMIT TRANSACTION;
    """;

    res = await self.db.query(query, { 'account': account, 'txns': txns })
    return res[1][0].id  -- $acc created record id (index depends on driver)
```

Why: minimizes network calls, keeps atomicity, leverages server-side mapping.

## 4) Query Patterns and API

- **Date-range per account**: `SELECT * FROM transaction WHERE account = $acc AND value_date >= $from AND value_date < $to ORDER BY value_date ASC LIMIT $limit START $offset;`
- **Full-text search**: `SELECT * FROM transaction WHERE account = $acc AND description @ $q LIMIT 50;`
- **Aggregations**: `SELECT math::sum(amount_minor) AS net, math::sum(if amount_minor > 0 THEN amount_minor ELSE 0 END) AS credits, math::sum(if amount_minor < 0 THEN -amount_minor ELSE 0 END) AS debits BY math::time::month(value_date) FROM transaction WHERE account = $acc AND value_date >= $from AND value_date < $to;`

Expose these via service methods with strong pagination and parameterization.

## 5) Rollups (optional, but recommended)

For frequent analytics, maintain daily or monthly rollups:

- Table `txn_rollup(account, period_start, credits_minor, debits_minor, net_minor)`
- Update on ingest (same transaction) or via an async worker.
- Query UI hits rollups first, falls back to raw when needed.

## 6) Connection Lifecycle and Pooling

- Create one `AsyncSurreal` per process and reuse.
- Initialize namespace/DB on app startup; close on shutdown.
- If the driver supports a transaction context (`async with db.transaction()`), prefer it. Else, keep multi-statement in one `query` call.

## 7) Repo and Models Organization

- Put Pydantic models in `upload_service/models.py` (or `domain/models.py`).
- Keep Surreal-specific repo code in `upload_service/upload_repo.py`.
- Convert models with `.model_dump()` and a converter to minor units and UTC datetimes.
- Keep read APIs in `analysis_service/analysis_service.py` with clear methods for date ranges, search, and aggregations.

## 8) Idempotency and Dedup

- Prevent duplicate account creation: unique index `(number, owner)`.
- Add an optional `upload_id` (UUID) per batch and store on `transaction` rows for traceability.
- Reject or merge if the same `upload_id` is seen again.

## 9) Observability

- Log Surreal timings per call.
- Add lightweight ingestion benchmarks (records/sec), and track slow queries with `LIMIT` + index hints (ensured by indexes above).

## 10) Safety and Validation

- Validate all incoming amounts and dates before DB call.
- Enforce `SCHEMAFULL` + `ASSERT` rules to catch inconsistencies early.

### To-dos

- [ ] Add SCHEMAFULL tables and fields in settings/surreal/schema.surql
- [ ] Define composite, owner, and full-text indexes
- [ ] Create Pydantic models in upload_service/models.py with money/date converters
- [ ] Replace per-row creates with single atomic bulk query
- [ ] Implement date-range, search, and aggregation repo methods
- [ ] Use single AsyncSurreal instance with startup/shutdown handlers
- [ ] Enforce uniqueness and optional upload_id to deduplicate batches
- [ ] Add optional daily/monthly rollup table and write-updaters
- [ ] Add timings logging and ingestion throughput metrics