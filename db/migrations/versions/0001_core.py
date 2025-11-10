from __future__ import annotations

from alembic import op

revision = "0001_core"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Extensions for UUID generation (if not already present)
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    # Core reference tables
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email TEXT UNIQUE NOT NULL,
            hashed_password TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
            is_verified BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS workspaces (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            provider TEXT,
            name TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    # Raw ingestion and cleaned transactions
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions_raw (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            blob JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    # Partitioned cleaned transactions
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions_cleaned (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            raw_id UUID NOT NULL REFERENCES transactions_raw(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            txn_date DATE NOT NULL,
            amount NUMERIC(18,2) NOT NULL,
            currency TEXT NOT NULL,
            merchant TEXT,
            raw_description TEXT,
            normalized_desc TEXT,
            merchant_id UUID,
            quality_flags JSONB DEFAULT '{}'::jsonb
        ) PARTITION BY RANGE (txn_date);
        """
    )
    # Current and next month partitions (rolling partitions can be added by jobs later)
    op.execute(
        """
        DO $$
        DECLARE
            start1 DATE := date_trunc('month', CURRENT_DATE)::date;
            start2 DATE := (date_trunc('month', CURRENT_DATE) + interval '1 month')::date;
            end1   DATE := (date_trunc('month', CURRENT_DATE) + interval '1 month')::date;
            end2   DATE := (date_trunc('month', CURRENT_DATE) + interval '2 month')::date;
            part1  TEXT := 'transactions_cleaned_' || to_char(start1, 'YYYY_MM');
            part2  TEXT := 'transactions_cleaned_' || to_char(start2, 'YYYY_MM');
        BEGIN
            EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF transactions_cleaned FOR VALUES FROM (%L) TO (%L);', part1, start1, end1);
            EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF transactions_cleaned FOR VALUES FROM (%L) TO (%L);', part2, start2, end2);
        END $$;
        """
    )
    # Helpful indexes
    op.execute("CREATE INDEX IF NOT EXISTS idx_txn_cleaned_user_date ON transactions_cleaned (user_id, txn_date DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_txn_cleaned_merchant ON transactions_cleaned (merchant);")

    # Merchants and categories
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS merchants (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            normalized_name TEXT
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            parent_id UUID REFERENCES categories(id) ON DELETE SET NULL
        );
        """
    )

    # Model outputs
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS model_predictions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            txn_id UUID NOT NULL REFERENCES transactions_cleaned(id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            version TEXT NOT NULL,
            label TEXT NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS anomalies (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            txn_id UUID NOT NULL REFERENCES transactions_cleaned(id) ON DELETE CASCADE,
            score DOUBLE PRECISION NOT NULL,
            reason JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    # Budgets
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS budgets (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            period TEXT NOT NULL, -- e.g., 'monthly'
            amount NUMERIC(18,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS budget_periods (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            budget_id UUID NOT NULL REFERENCES budgets(id) ON DELETE CASCADE,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            spent NUMERIC(18,2) NOT NULL DEFAULT 0
        );
        """
    )

    # Feedback
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS user_feedback (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            txn_id UUID NOT NULL REFERENCES transactions_cleaned(id) ON DELETE CASCADE,
            kind TEXT NOT NULL, -- categorization, anomaly, etc.
            chosen_label TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    # Outbox
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS outbox_events (
            id BIGSERIAL PRIMARY KEY,
            kind TEXT NOT NULL,
            aggregate_id UUID NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            processed_at TIMESTAMPTZ
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed ON outbox_events (processed_at) WHERE processed_at IS NULL;")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outbox_events;")
    op.execute("DROP TABLE IF EXISTS user_feedback;")
    op.execute("DROP TABLE IF EXISTS budget_periods;")
    op.execute("DROP TABLE IF EXISTS budgets;")
    op.execute("DROP TABLE IF EXISTS anomalies;")
    op.execute("DROP TABLE IF EXISTS model_predictions;")
    op.execute("DROP TABLE IF EXISTS categories;")
    op.execute("DROP TABLE IF EXISTS merchants;")
    op.execute("DROP TABLE IF EXISTS transactions_cleaned CASCADE;")
    op.execute("DROP TABLE IF EXISTS transactions_raw;")
    op.execute("DROP TABLE IF EXISTS accounts;")
    op.execute("DROP TABLE IF EXISTS workspaces;")
    op.execute("DROP TABLE IF EXISTS users;")


