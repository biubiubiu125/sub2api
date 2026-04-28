-- P2 hardening for custom referral order snapshots, audit, and wallet ledger.
-- Run the read-only checks in the rollout report before applying this migration.

ALTER TABLE payment_orders
    ADD COLUMN IF NOT EXISTS custom_referral_affiliate_id BIGINT NULL REFERENCES custom_affiliates(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS custom_referral_rate DECIMAL(10,4) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS custom_referral_commission_status VARCHAR(32) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS custom_referral_commission_error TEXT NULL,
    ADD COLUMN IF NOT EXISTS custom_referral_commission_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_payment_orders_custom_referral_affiliate_id
    ON payment_orders(custom_referral_affiliate_id);
CREATE INDEX IF NOT EXISTS idx_payment_orders_custom_referral_commission_status
    ON payment_orders(custom_referral_commission_status);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'i'
          AND c.relname = 'ux_custom_commission_withdrawals_idempotency_key'
    ) THEN
        RAISE EXCEPTION 'missing ux_custom_commission_withdrawals_idempotency_key; apply/repair migration 136 before enabling withdrawal POST idempotency';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM custom_referral_bindings
        WHERE invitee_user_id = inviter_user_id
    ) THEN
        RAISE EXCEPTION 'self custom_referral_bindings found; clean invitee_user_id = inviter_user_id rows before adding DB guard';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_referral_bindings_no_self'
    ) THEN
        ALTER TABLE custom_referral_bindings
            ADD CONSTRAINT chk_custom_referral_bindings_no_self
            CHECK (invitee_user_id <> inviter_user_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_payment_orders_custom_referral_snapshot_amounts'
    ) THEN
        ALTER TABLE payment_orders
            ADD CONSTRAINT chk_payment_orders_custom_referral_snapshot_amounts
            CHECK (
                commission_base_amount >= 0
                AND custom_referral_rate >= 0
            );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS custom_referral_admin_audit_logs (
    id BIGSERIAL PRIMARY KEY,
    action VARCHAR(64) NOT NULL,
    target_user_id BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    affiliate_id BIGINT NULL REFERENCES custom_affiliates(id) ON DELETE SET NULL,
    admin_user_id BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    reason TEXT NOT NULL DEFAULT '',
    ip VARCHAR(64) NOT NULL DEFAULT '',
    user_agent TEXT NOT NULL DEFAULT '',
    old_value JSONB NOT NULL DEFAULT '{}'::jsonb,
    new_value JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_referral_admin_audit_logs_action_created
    ON custom_referral_admin_audit_logs(action, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_custom_referral_admin_audit_logs_target_user
    ON custom_referral_admin_audit_logs(target_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_custom_referral_admin_audit_logs_admin_user
    ON custom_referral_admin_audit_logs(admin_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS user_balance_ledger (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type VARCHAR(64) NOT NULL,
    ref_type VARCHAR(64) NOT NULL DEFAULT '',
    ref_id VARCHAR(128) NOT NULL DEFAULT '',
    external_ref_id VARCHAR(160) NOT NULL DEFAULT '',
    delta_amount DECIMAL(20,8) NOT NULL,
    balance_before DECIMAL(20,8) NOT NULL,
    balance_after DECIMAL(20,8) NOT NULL,
    remark TEXT NOT NULL DEFAULT '',
    operator VARCHAR(64) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM user_balance_ledger
        WHERE external_ref_id <> ''
        GROUP BY type, external_ref_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate user_balance_ledger business refs found; check type/external_ref_id duplicates before creating unique index';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_user_balance_ledger_user_created
    ON user_balance_ledger(user_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_balance_ledger_business_ref
    ON user_balance_ledger(type, external_ref_id)
    WHERE external_ref_id <> '';
