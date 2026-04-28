-- Add user-submitted withdrawal idempotency without touching legacy upstream affiliate tables.

ALTER TABLE custom_commission_withdrawals
    ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128) NOT NULL DEFAULT '';

UPDATE custom_commission_withdrawals
SET idempotency_key = ''
WHERE idempotency_key IS NULL;

ALTER TABLE custom_commission_withdrawals
    ALTER COLUMN idempotency_key SET DEFAULT '',
    ALTER COLUMN idempotency_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_commission_withdrawals_idempotency_key
    ON custom_commission_withdrawals(affiliate_id, idempotency_key)
    WHERE idempotency_key <> '';

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
        RAISE EXCEPTION 'duplicate custom_commission_ledger business refs found; run duplicate check SQL before applying ux_custom_commission_ledger_business_ref';
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
