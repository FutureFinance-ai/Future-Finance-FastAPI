-- Scope for backend-minted tokens that embed { tenant, user } claims
DEFINE SCOPE app
  SESSION 1d
  SIGNIN ( RETURN { tenant: $tenant, user: $user } );

-- Core domain tables (graph-first)
DEFINE TABLE merchant SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD name             ON merchant TYPE string;
DEFINE FIELD normalized_name  ON merchant TYPE string;
DEFINE FIELD provider_ids     ON merchant TYPE option<array<string>>;
DEFINE FIELD category_hint    ON merchant TYPE option<string>;
DEFINE INDEX merchant_name_idx ON merchant FIELDS normalized_name;

DEFINE TABLE category SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD code             ON category TYPE string;
DEFINE FIELD name             ON category TYPE string;
DEFINE FIELD parent           ON category TYPE option<record<category>>;

-- Extend existing account/transaction model with AI fields if not present
DEFINE FIELD merchant_name    ON transaction TYPE option<string>;
DEFINE FIELD is_subscription  ON transaction TYPE bool DEFAULT false;
DEFINE FIELD sequence_index   ON transaction TYPE option<int>;
DEFINE FIELD idempotency_key  ON transaction TYPE option<string>;
DEFINE FIELD ingested_at      ON transaction TYPE option<datetime> VALUE $before OR time::now();

DEFINE INDEX transaction_account_date ON transaction FIELDS account, trans_time, sequence_index;

-- Graph relations
DEFINE TABLE purchased_at SCHEMAFULL TYPE RELATION FROM transaction TO merchant PERMISSIONS FULL;
DEFINE TABLE tagged_as    SCHEMAFULL TYPE RELATION FROM transaction TO category PERMISSIONS FULL;
DEFINE TABLE follows      SCHEMAFULL TYPE RELATION FROM transaction TO transaction PERMISSIONS FULL;
DEFINE FIELD account      ON follows TYPE record<account>;
DEFINE FIELD order_index  ON follows TYPE int;

-- Subscriptions
DEFINE TABLE subscription SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD user             ON subscription TYPE record<users>;
DEFINE FIELD merchant         ON subscription TYPE record<merchant>;
DEFINE FIELD account          ON subscription TYPE record<account>;
DEFINE FIELD amount_estimate  ON subscription TYPE option<int>;
DEFINE FIELD currency         ON subscription TYPE option<string>;
DEFINE FIELD cadence          ON subscription TYPE option<string> ASSERT $value IN ['weekly','monthly','yearly','other'];
DEFINE FIELD active           ON subscription TYPE bool DEFAULT true;
DEFINE FIELD first_seen       ON subscription TYPE option<datetime>;
DEFINE FIELD last_seen        ON subscription TYPE option<datetime>;
DEFINE INDEX subscription_user_active_idx ON subscription FIELDS user, active;

-- Insights
DEFINE TABLE insight SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD user         ON insight TYPE record<users>;
DEFINE FIELD account      ON insight TYPE option<record<account>>;
DEFINE FIELD type         ON insight TYPE string;
DEFINE FIELD message      ON insight TYPE string;
DEFINE FIELD score        ON insight TYPE option<number>;
DEFINE FIELD period_start ON insight TYPE option<datetime>;
DEFINE FIELD period_end   ON insight TYPE option<datetime>;
DEFINE FIELD data         ON insight TYPE option<object>;
DEFINE FIELD created_at   ON insight TYPE datetime VALUE $before OR time::now();
DEFINE INDEX insight_user_period_idx ON insight FIELDS user, period_start, period_end;

-- Forecasts
DEFINE TABLE forecast SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD user          ON forecast TYPE record<users>;
DEFINE FIELD account       ON forecast TYPE record<account>;
DEFINE FIELD horizon_days  ON forecast TYPE int;
DEFINE FIELD generated_at  ON forecast TYPE datetime VALUE $before OR time::now();
DEFINE FIELD method        ON forecast TYPE string;
DEFINE FIELD version       ON forecast TYPE option<string>;
DEFINE FIELD points        ON forecast TYPE array<object>;


