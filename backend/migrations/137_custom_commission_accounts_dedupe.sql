-- Enforce one custom_commission_accounts row per affiliate/user.
--
-- This migration intentionally does not merge or delete duplicate rows. Duplicate
-- commission accounts are financial data that must be reviewed with the
-- preflight report and reconciled manually before this migration is applied.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM custom_commission_accounts
        WHERE affiliate_id IS NOT NULL
        GROUP BY affiliate_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate custom_commission_accounts.affiliate_id rows found; run backend/migrations/preflight/custom_referral_preflight.sql and reconcile accounts manually before applying migration 137';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM custom_commission_accounts
        WHERE user_id IS NOT NULL
        GROUP BY user_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'duplicate custom_commission_accounts.user_id rows found; run backend/migrations/preflight/custom_referral_preflight.sql and reconcile accounts manually before applying migration 137';
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_commission_accounts_affiliate_id
    ON custom_commission_accounts(affiliate_id)
    WHERE affiliate_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_commission_accounts_user_id
    ON custom_commission_accounts(user_id)
    WHERE user_id IS NOT NULL;
