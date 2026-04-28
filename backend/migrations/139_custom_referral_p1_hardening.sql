-- P1 hardening for the custom referral chain.
-- This migration intentionally fails when historical duplicate ledger keys or
-- illegal negative balances exist. Run the read-only SQL in the rollout SOP
-- before applying it to production.

ALTER TABLE payment_orders
    ADD COLUMN IF NOT EXISTS commission_base_amount DECIMAL(20,2) NOT NULL DEFAULT 0;

UPDATE payment_orders
SET commission_base_amount = CASE
    WHEN order_type = 'subscription' THEN amount
    WHEN order_type = 'balance' AND pay_amount > 0 AND fee_rate > 0 THEN ROUND(pay_amount / (1 + fee_rate / 100), 2)
    WHEN order_type = 'balance' AND pay_amount > 0 THEN pay_amount
    ELSE amount
END
WHERE commission_base_amount = 0;

CREATE TABLE IF NOT EXISTS custom_commission_reversals (
    id BIGSERIAL PRIMARY KEY,
    affiliate_id BIGINT NOT NULL REFERENCES custom_affiliates(id) ON DELETE CASCADE,
    commission_id BIGINT NOT NULL REFERENCES custom_referral_commissions(id) ON DELETE CASCADE,
    order_id BIGINT NOT NULL REFERENCES payment_orders(id) ON DELETE CASCADE,
    admin_user_id BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    external_ref_id VARCHAR(128) NOT NULL,
    refund_amount DECIMAL(20,8) NOT NULL,
    reverse_amount DECIMAL(20,8) NOT NULL,
    delta_pending DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_available DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_frozen DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_reversed DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_debt DECIMAL(20,8) NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_commission_reversals_external_ref_id
    ON custom_commission_reversals(external_ref_id);
CREATE INDEX IF NOT EXISTS idx_custom_commission_reversals_commission_id
    ON custom_commission_reversals(commission_id);
CREATE INDEX IF NOT EXISTS idx_custom_commission_reversals_order_id
    ON custom_commission_reversals(order_id);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM custom_commission_ledger
        WHERE external_ref_id <> ''
          AND type IN (
              'commission_accrue',
              'commission_settle',
              'commission_reverse',
              'withdrawal_apply',
              'withdrawal_paid',
              'withdrawal_cancel',
              'withdrawal_reject'
          )
        GROUP BY type, external_ref_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate custom_commission_ledger business refs found; check type/external_ref_id duplicates before creating ux_custom_commission_ledger_business_ref';
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_commission_ledger_business_ref
    ON custom_commission_ledger(type, external_ref_id)
    WHERE external_ref_id <> ''
      AND type IN (
          'commission_accrue',
          'commission_settle',
          'commission_reverse',
          'withdrawal_apply',
          'withdrawal_paid',
          'withdrawal_cancel',
          'withdrawal_reject'
      );

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM custom_commission_accounts
        WHERE pending_amount < 0
           OR available_amount < 0
           OR frozen_amount < 0
           OR withdrawn_amount < 0
           OR reversed_amount < 0
           OR debt_amount < 0
    ) THEN
        RAISE EXCEPTION 'negative custom_commission_accounts balances found; run reconciliation SQL before adding non-negative checks';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM custom_referral_commissions
        WHERE base_amount < 0
           OR rate < 0
           OR commission_amount < 0
           OR refunded_amount < 0
           OR refunded_amount > commission_amount + 0.00000001
    ) THEN
        RAISE EXCEPTION 'invalid custom_referral_commissions amount fields found; run reconciliation SQL before adding checks';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM custom_commission_withdrawals
        WHERE amount < 0
           OR fee_amount < 0
           OR net_amount < 0
    ) THEN
        RAISE EXCEPTION 'invalid custom_commission_withdrawals amount fields found; run reconciliation SQL before adding checks';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM custom_commission_withdrawal_items
        WHERE allocated_amount < 0
    ) THEN
        RAISE EXCEPTION 'invalid custom_commission_withdrawal_items allocated_amount found; run reconciliation SQL before adding checks';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM custom_referral_bindings
        WHERE invitee_user_id = inviter_user_id
    ) THEN
        RAISE EXCEPTION 'self custom_referral_bindings found; clean data before adding no-self check';
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_payment_orders_commission_base_amount_non_negative'
    ) THEN
        ALTER TABLE payment_orders
            ADD CONSTRAINT chk_payment_orders_commission_base_amount_non_negative
            CHECK (commission_base_amount >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_commission_accounts_non_negative'
    ) THEN
        ALTER TABLE custom_commission_accounts
            ADD CONSTRAINT chk_custom_commission_accounts_non_negative
            CHECK (
                pending_amount >= 0
                AND available_amount >= 0
                AND frozen_amount >= 0
                AND withdrawn_amount >= 0
                AND reversed_amount >= 0
                AND debt_amount >= 0
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_referral_commissions_amounts'
    ) THEN
        ALTER TABLE custom_referral_commissions
            ADD CONSTRAINT chk_custom_referral_commissions_amounts
            CHECK (
                base_amount >= 0
                AND rate >= 0
                AND commission_amount >= 0
                AND refunded_amount >= 0
                AND refunded_amount <= commission_amount + 0.00000001
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_commission_withdrawals_amounts'
    ) THEN
        ALTER TABLE custom_commission_withdrawals
            ADD CONSTRAINT chk_custom_commission_withdrawals_amounts
            CHECK (amount >= 0 AND fee_amount >= 0 AND net_amount >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_commission_withdrawal_items_amount'
    ) THEN
        ALTER TABLE custom_commission_withdrawal_items
            ADD CONSTRAINT chk_custom_commission_withdrawal_items_amount
            CHECK (allocated_amount >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_referral_bindings_no_self'
    ) THEN
        ALTER TABLE custom_referral_bindings
            ADD CONSTRAINT chk_custom_referral_bindings_no_self
            CHECK (invitee_user_id <> inviter_user_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_commission_reversals_amounts'
    ) THEN
        ALTER TABLE custom_commission_reversals
            ADD CONSTRAINT chk_custom_commission_reversals_amounts
            CHECK (
                refund_amount >= 0
                AND reverse_amount >= 0
                AND delta_reversed >= 0
                AND delta_debt >= 0
            );
    END IF;
END $$;
