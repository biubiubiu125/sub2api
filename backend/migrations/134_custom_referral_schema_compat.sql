-- Bring partially-created custom referral tables up to the current schema.
-- Some restored deployments may already have early custom_* tables recorded as
-- migrated, so approval must not depend on manually repairing those tables.

ALTER TABLE custom_affiliates
    ADD COLUMN IF NOT EXISTS source_type VARCHAR(32) NOT NULL DEFAULT 'admin_created',
    ADD COLUMN IF NOT EXISTS rate_override DECIMAL(10,4) NULL,
    ADD COLUMN IF NOT EXISTS acquisition_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS settlement_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS withdrawal_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS risk_reason TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS risk_note TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS approved_by BIGINT NULL,
    ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS disabled_by BIGINT NULL,
    ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE custom_affiliates
SET source_type = 'admin_created'
WHERE source_type IS NULL OR source_type = '';

UPDATE custom_affiliates
SET risk_reason = ''
WHERE risk_reason IS NULL;

UPDATE custom_affiliates
SET risk_note = ''
WHERE risk_note IS NULL;

UPDATE custom_affiliates
SET acquisition_enabled = TRUE
WHERE acquisition_enabled IS NULL;

UPDATE custom_affiliates
SET settlement_enabled = TRUE
WHERE settlement_enabled IS NULL;

UPDATE custom_affiliates
SET withdrawal_enabled = TRUE
WHERE withdrawal_enabled IS NULL;

UPDATE custom_affiliates
SET created_at = NOW()
WHERE created_at IS NULL;

UPDATE custom_affiliates
SET updated_at = NOW()
WHERE updated_at IS NULL;

ALTER TABLE custom_affiliates
    ALTER COLUMN source_type SET DEFAULT 'admin_created',
    ALTER COLUMN source_type SET NOT NULL,
    ALTER COLUMN acquisition_enabled SET DEFAULT TRUE,
    ALTER COLUMN acquisition_enabled SET NOT NULL,
    ALTER COLUMN settlement_enabled SET DEFAULT TRUE,
    ALTER COLUMN settlement_enabled SET NOT NULL,
    ALTER COLUMN withdrawal_enabled SET DEFAULT TRUE,
    ALTER COLUMN withdrawal_enabled SET NOT NULL,
    ALTER COLUMN risk_reason SET DEFAULT '',
    ALTER COLUMN risk_reason SET NOT NULL,
    ALTER COLUMN risk_note SET DEFAULT '',
    ALTER COLUMN risk_note SET NOT NULL,
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_custom_affiliates_status ON custom_affiliates(status);
CREATE INDEX IF NOT EXISTS idx_custom_affiliates_acquisition_enabled ON custom_affiliates(acquisition_enabled);
CREATE INDEX IF NOT EXISTS idx_custom_affiliates_settlement_enabled ON custom_affiliates(settlement_enabled);
CREATE INDEX IF NOT EXISTS idx_custom_affiliates_withdrawal_enabled ON custom_affiliates(withdrawal_enabled);

CREATE TABLE IF NOT EXISTS custom_commission_accounts (
    affiliate_id BIGINT PRIMARY KEY REFERENCES custom_affiliates(id) ON DELETE CASCADE
);

ALTER TABLE custom_commission_accounts
    ADD COLUMN IF NOT EXISTS pending_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS available_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS frozen_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS withdrawn_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reversed_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS debt_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE custom_commission_accounts
SET pending_amount = 0
WHERE pending_amount IS NULL;

UPDATE custom_commission_accounts
SET available_amount = 0
WHERE available_amount IS NULL;

UPDATE custom_commission_accounts
SET frozen_amount = 0
WHERE frozen_amount IS NULL;

UPDATE custom_commission_accounts
SET withdrawn_amount = 0
WHERE withdrawn_amount IS NULL;

UPDATE custom_commission_accounts
SET reversed_amount = 0
WHERE reversed_amount IS NULL;

UPDATE custom_commission_accounts
SET debt_amount = 0
WHERE debt_amount IS NULL;

UPDATE custom_commission_accounts
SET created_at = NOW()
WHERE created_at IS NULL;

UPDATE custom_commission_accounts
SET updated_at = NOW()
WHERE updated_at IS NULL;

ALTER TABLE custom_commission_accounts
    ALTER COLUMN pending_amount SET DEFAULT 0,
    ALTER COLUMN pending_amount SET NOT NULL,
    ALTER COLUMN available_amount SET DEFAULT 0,
    ALTER COLUMN available_amount SET NOT NULL,
    ALTER COLUMN frozen_amount SET DEFAULT 0,
    ALTER COLUMN frozen_amount SET NOT NULL,
    ALTER COLUMN withdrawn_amount SET DEFAULT 0,
    ALTER COLUMN withdrawn_amount SET NOT NULL,
    ALTER COLUMN reversed_amount SET DEFAULT 0,
    ALTER COLUMN reversed_amount SET NOT NULL,
    ALTER COLUMN debt_amount SET DEFAULT 0,
    ALTER COLUMN debt_amount SET NOT NULL,
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at SET NOT NULL;
