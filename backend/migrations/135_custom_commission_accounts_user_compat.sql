-- Align legacy custom_commission_accounts rows with the current affiliate_id lookup path.
-- Early custom referral deployments used user_id as the account key; current code uses affiliate_id.

ALTER TABLE custom_commission_accounts
    ADD COLUMN IF NOT EXISTS affiliate_id BIGINT NULL,
    ADD COLUMN IF NOT EXISTS user_id BIGINT NULL;

UPDATE custom_commission_accounts ca
SET affiliate_id = a.id
FROM custom_affiliates a
WHERE ca.affiliate_id IS NULL
  AND ca.user_id = a.user_id;

UPDATE custom_commission_accounts ca
SET user_id = a.user_id
FROM custom_affiliates a
WHERE ca.user_id IS NULL
  AND ca.affiliate_id = a.id;

INSERT INTO custom_commission_accounts (affiliate_id, user_id, created_at, updated_at)
SELECT a.id, a.user_id, NOW(), NOW()
FROM custom_affiliates a
WHERE NOT EXISTS (
    SELECT 1
    FROM custom_commission_accounts ca
    WHERE ca.affiliate_id = a.id
)
  AND NOT EXISTS (
    SELECT 1
    FROM custom_commission_accounts ca
    WHERE ca.user_id = a.user_id
);

CREATE INDEX IF NOT EXISTS idx_custom_commission_accounts_affiliate_id
    ON custom_commission_accounts(affiliate_id);

CREATE INDEX IF NOT EXISTS idx_custom_commission_accounts_user_id
    ON custom_commission_accounts(user_id);
