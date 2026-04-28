-- Custom referral final hardening:
-- 1) durable commission job outbox for order completion -> commission creation.
-- 2) idempotent admin commission adjustment ledger keys.
-- This migration intentionally fails if historical adjustment duplicate keys exist.

CREATE TABLE IF NOT EXISTS custom_referral_commission_jobs (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES payment_orders(id) ON DELETE CASCADE,
    affiliate_id BIGINT NULL REFERENCES custom_affiliates(id) ON DELETE SET NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    locked_at TIMESTAMPTZ NULL,
    succeeded_at TIMESTAMPTZ NULL,
    failed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_referral_commission_jobs_order_id
    ON custom_referral_commission_jobs(order_id);

CREATE INDEX IF NOT EXISTS idx_custom_referral_commission_jobs_status_updated_at
    ON custom_referral_commission_jobs(status, updated_at);

CREATE INDEX IF NOT EXISTS idx_custom_referral_commission_jobs_affiliate_id
    ON custom_referral_commission_jobs(affiliate_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_referral_commission_jobs_status'
    ) THEN
        ALTER TABLE custom_referral_commission_jobs
            ADD CONSTRAINT chk_custom_referral_commission_jobs_status
            CHECK (status IN ('pending', 'processing', 'succeeded', 'failed'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_custom_referral_commission_jobs_attempt_count'
    ) THEN
        ALTER TABLE custom_referral_commission_jobs
            ADD CONSTRAINT chk_custom_referral_commission_jobs_attempt_count
            CHECK (attempt_count >= 0);
    END IF;
END $$;

INSERT INTO custom_referral_commission_jobs (
    order_id, affiliate_id, status, attempt_count, last_error, succeeded_at, failed_at, created_at, updated_at
)
SELECT
    po.id,
    po.custom_referral_affiliate_id,
    CASE
        WHEN po.custom_referral_commission_status = 'succeeded' THEN 'succeeded'
        WHEN po.custom_referral_commission_status = 'failed' THEN 'failed'
        ELSE 'pending'
    END,
    0,
    COALESCE(po.custom_referral_commission_error, ''),
    CASE WHEN po.custom_referral_commission_status = 'succeeded' THEN po.custom_referral_commission_at ELSE NULL END,
    CASE WHEN po.custom_referral_commission_status = 'failed' THEN COALESCE(po.custom_referral_commission_at, NOW()) ELSE NULL END,
    NOW(),
    NOW()
FROM payment_orders po
WHERE po.status = 'COMPLETED'
  AND po.custom_referral_affiliate_id IS NOT NULL
  AND po.custom_referral_rate > 0
  AND po.commission_base_amount > 0
ON CONFLICT (order_id) DO NOTHING;

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
              'withdrawal_reject',
              'commission_adjust_increase',
              'commission_adjust_decrease'
          )
        GROUP BY type, external_ref_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate custom_commission_ledger business refs found before expanding adjustment idempotency';
    END IF;
END $$;

DROP INDEX IF EXISTS ux_custom_commission_ledger_business_ref;

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
          'withdrawal_reject',
          'commission_adjust_increase',
          'commission_adjust_decrease'
      );
