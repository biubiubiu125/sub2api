-- Custom referral production preflight checks.
-- Run this file in read-only mode before applying migrations 136-142.
-- Every query should return zero rows. Any returned row requires manual cleanup.

-- 1. Ledger business idempotency duplicates.
SELECT type, external_ref_id, COUNT(*) AS duplicate_count
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
HAVING COUNT(*) > 1;

-- 2. Manual commission reversal idempotency duplicates.
SELECT external_ref_id, COUNT(*) AS duplicate_count
FROM custom_commission_reversals
WHERE external_ref_id <> ''
GROUP BY external_ref_id
HAVING COUNT(*) > 1;

-- 3. Illegal negative commission account amounts.
SELECT *
FROM custom_commission_accounts
WHERE pending_amount < 0
   OR available_amount < 0
   OR frozen_amount < 0
   OR withdrawn_amount < 0
   OR reversed_amount < 0
   OR debt_amount < 0;

-- 4. Duplicate commission accounts by affiliate.
SELECT affiliate_id, COUNT(*) AS duplicate_count
FROM custom_commission_accounts
WHERE affiliate_id IS NOT NULL
GROUP BY affiliate_id
HAVING COUNT(*) > 1;

-- 5. Duplicate commission accounts by user.
SELECT user_id, COUNT(*) AS duplicate_count
FROM custom_commission_accounts
WHERE user_id IS NOT NULL
GROUP BY user_id
HAVING COUNT(*) > 1;

-- 6. Duplicate payment order out_trade_no values.
SELECT out_trade_no, COUNT(*) AS duplicate_count
FROM payment_orders
WHERE out_trade_no <> ''
GROUP BY out_trade_no
HAVING COUNT(*) > 1;

-- 7. Duplicate payment provider trade_no values.
SELECT payment_trade_no, COUNT(*) AS duplicate_count
FROM payment_orders
WHERE payment_trade_no <> ''
GROUP BY payment_trade_no
HAVING COUNT(*) > 1;

-- 8. Direct self-invite rows.
SELECT id, invitee_user_id, inviter_user_id, affiliate_id
FROM custom_referral_bindings
WHERE invitee_user_id = inviter_user_id;

-- 9. Cyclic referral candidates.
WITH RECURSIVE referral_path(root_user_id, current_user_id, depth, path) AS (
    SELECT
        b.invitee_user_id,
        b.inviter_user_id,
        1,
        ARRAY[b.invitee_user_id, b.inviter_user_id]
    FROM custom_referral_bindings b
    UNION ALL
    SELECT
        referral_path.root_user_id,
        b.inviter_user_id,
        referral_path.depth + 1,
        referral_path.path || b.inviter_user_id
    FROM custom_referral_bindings b
    JOIN referral_path ON b.invitee_user_id = referral_path.current_user_id
    WHERE referral_path.depth < 64
      AND (b.inviter_user_id = referral_path.root_user_id OR NOT b.inviter_user_id = ANY(referral_path.path))
)
SELECT root_user_id, current_user_id, depth, path
FROM referral_path
WHERE current_user_id = root_user_id
   OR depth >= 64;

-- 10. Commission reversal debt consistency.
SELECT affiliate_id, debt_amount, reversed_amount
FROM custom_commission_accounts
WHERE debt_amount < 0
   OR reversed_amount < 0
   OR debt_amount > reversed_amount + 0.00000001;

-- 11. Completed orders with referral snapshot but no durable commission job.
SELECT po.id, po.custom_referral_affiliate_id, po.custom_referral_commission_status
FROM payment_orders po
LEFT JOIN custom_referral_commission_jobs job ON job.order_id = po.id
WHERE po.status = 'COMPLETED'
  AND po.custom_referral_affiliate_id IS NOT NULL
  AND po.custom_referral_rate > 0
  AND po.commission_base_amount > 0
  AND job.id IS NULL;

-- 12. Critical unique index definition checks.
WITH required_index_parts(indexname, required_part) AS (
    VALUES
        ('ux_custom_commission_ledger_business_ref', 'custom_commission_ledger'),
        ('ux_custom_commission_ledger_business_ref', 'external_ref_id'),
        ('ux_custom_commission_ledger_business_ref', 'type'),
        ('ux_custom_commission_reversals_external_ref_id', 'custom_commission_reversals'),
        ('ux_custom_commission_reversals_external_ref_id', 'external_ref_id'),
        ('ux_custom_commission_accounts_affiliate_id', 'custom_commission_accounts'),
        ('ux_custom_commission_accounts_affiliate_id', 'affiliate_id'),
        ('ux_custom_commission_accounts_user_id', 'custom_commission_accounts'),
        ('ux_custom_commission_accounts_user_id', 'user_id'),
        ('paymentorder_out_trade_no_unique', 'payment_orders'),
        ('paymentorder_out_trade_no_unique', 'out_trade_no'),
        ('paymentorder_payment_trade_no_unique', 'payment_orders'),
        ('paymentorder_payment_trade_no_unique', 'payment_trade_no'),
        ('ux_custom_referral_commission_jobs_order_id', 'custom_referral_commission_jobs'),
        ('ux_custom_referral_commission_jobs_order_id', 'order_id')
)
SELECT required.indexname, required.required_part, indexes.indexdef
FROM required_index_parts required
LEFT JOIN pg_indexes indexes
    ON indexes.schemaname = 'public'
   AND indexes.indexname = required.indexname
WHERE indexes.indexname IS NULL
   OR indexes.indexdef NOT ILIKE '%' || required.required_part || '%';
