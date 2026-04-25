CREATE TABLE IF NOT EXISTS custom_affiliates (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    invite_code VARCHAR(32) NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    source_type VARCHAR(32) NOT NULL DEFAULT 'admin_created',
    rate_override DECIMAL(10,4) NULL,
    acquisition_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    settlement_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    withdrawal_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    risk_reason TEXT NOT NULL DEFAULT '',
    risk_note TEXT NOT NULL DEFAULT '',
    approved_by BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    approved_at TIMESTAMPTZ NULL,
    disabled_by BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    disabled_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_affiliates_status ON custom_affiliates(status);
CREATE INDEX IF NOT EXISTS idx_custom_affiliates_acquisition_enabled ON custom_affiliates(acquisition_enabled);
CREATE INDEX IF NOT EXISTS idx_custom_affiliates_settlement_enabled ON custom_affiliates(settlement_enabled);
CREATE INDEX IF NOT EXISTS idx_custom_affiliates_withdrawal_enabled ON custom_affiliates(withdrawal_enabled);

CREATE TABLE IF NOT EXISTS custom_referral_bindings (
    id BIGSERIAL PRIMARY KEY,
    invitee_user_id BIGINT NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    inviter_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    affiliate_id BIGINT NOT NULL REFERENCES custom_affiliates(id) ON DELETE CASCADE,
    bind_source VARCHAR(32) NOT NULL DEFAULT 'cookie',
    bind_code VARCHAR(32) NOT NULL,
    bound_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_referral_bindings_affiliate_id ON custom_referral_bindings(affiliate_id);
CREATE INDEX IF NOT EXISTS idx_custom_referral_bindings_inviter_user_id ON custom_referral_bindings(inviter_user_id);

CREATE TABLE IF NOT EXISTS custom_referral_clicks (
    id BIGSERIAL PRIMARY KEY,
    affiliate_id BIGINT NOT NULL REFERENCES custom_affiliates(id) ON DELETE CASCADE,
    invite_code VARCHAR(32) NOT NULL,
    referer TEXT NOT NULL DEFAULT '',
    landing_path TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_referral_clicks_affiliate_id ON custom_referral_clicks(affiliate_id);
CREATE INDEX IF NOT EXISTS idx_custom_referral_clicks_created_at ON custom_referral_clicks(created_at);

CREATE TABLE IF NOT EXISTS custom_commission_accounts (
    affiliate_id BIGINT PRIMARY KEY REFERENCES custom_affiliates(id) ON DELETE CASCADE,
    pending_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    available_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    frozen_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    withdrawn_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    reversed_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    debt_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS custom_referral_commissions (
    id BIGSERIAL PRIMARY KEY,
    affiliate_id BIGINT NOT NULL REFERENCES custom_affiliates(id) ON DELETE CASCADE,
    invitee_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    order_id BIGINT NOT NULL UNIQUE REFERENCES payment_orders(id) ON DELETE CASCADE,
    order_type VARCHAR(32) NOT NULL,
    base_amount DECIMAL(20,8) NOT NULL,
    rate DECIMAL(10,4) NOT NULL,
    commission_amount DECIMAL(20,8) NOT NULL,
    refunded_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    settle_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NULL,
    reversed_at TIMESTAMPTZ NULL,
    reversed_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_referral_commissions_affiliate_id ON custom_referral_commissions(affiliate_id);
CREATE INDEX IF NOT EXISTS idx_custom_referral_commissions_status ON custom_referral_commissions(status);
CREATE INDEX IF NOT EXISTS idx_custom_referral_commissions_settle_at ON custom_referral_commissions(settle_at);
CREATE INDEX IF NOT EXISTS idx_custom_referral_commissions_invitee_user_id ON custom_referral_commissions(invitee_user_id);

CREATE TABLE IF NOT EXISTS custom_commission_ledger (
    id BIGSERIAL PRIMARY KEY,
    affiliate_id BIGINT NOT NULL REFERENCES custom_affiliates(id) ON DELETE CASCADE,
    commission_id BIGINT NULL REFERENCES custom_referral_commissions(id) ON DELETE SET NULL,
    withdrawal_id BIGINT NULL,
    type VARCHAR(32) NOT NULL,
    ref_type VARCHAR(32) NOT NULL,
    ref_id VARCHAR(64) NOT NULL,
    external_ref_id VARCHAR(128) NOT NULL DEFAULT '',
    delta_pending DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_available DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_frozen DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_withdrawn DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_reversed DECIMAL(20,8) NOT NULL DEFAULT 0,
    delta_debt DECIMAL(20,8) NOT NULL DEFAULT 0,
    remark TEXT NOT NULL DEFAULT '',
    operator VARCHAR(64) NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_commission_ledger_affiliate_id ON custom_commission_ledger(affiliate_id);
CREATE INDEX IF NOT EXISTS idx_custom_commission_ledger_type ON custom_commission_ledger(type);
CREATE INDEX IF NOT EXISTS idx_custom_commission_ledger_external_ref_id ON custom_commission_ledger(external_ref_id);

CREATE TABLE IF NOT EXISTS custom_commission_withdrawals (
    id BIGSERIAL PRIMARY KEY,
    affiliate_id BIGINT NOT NULL REFERENCES custom_affiliates(id) ON DELETE CASCADE,
    amount DECIMAL(20,8) NOT NULL,
    fee_amount DECIMAL(20,8) NOT NULL DEFAULT 0,
    net_amount DECIMAL(20,8) NOT NULL,
    account_type VARCHAR(32) NOT NULL,
    account_name VARCHAR(128) NOT NULL DEFAULT '',
    account_no TEXT NOT NULL DEFAULT '',
    account_network VARCHAR(32) NOT NULL DEFAULT '',
    qr_image_url TEXT NOT NULL DEFAULT '',
    contact_info TEXT NOT NULL DEFAULT '',
    applicant_note TEXT NOT NULL DEFAULT '',
    admin_note TEXT NOT NULL DEFAULT '',
    payment_proof_url TEXT NOT NULL DEFAULT '',
    payment_txn_no TEXT NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    approved_at TIMESTAMPTZ NULL,
    payout_deadline_at TIMESTAMPTZ NULL,
    paid_at TIMESTAMPTZ NULL,
    rejected_at TIMESTAMPTZ NULL,
    canceled_at TIMESTAMPTZ NULL,
    reviewed_by BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    rejected_by BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    canceled_by BIGINT NULL REFERENCES users(id) ON DELETE SET NULL,
    reject_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_commission_withdrawals_affiliate_id ON custom_commission_withdrawals(affiliate_id);
CREATE INDEX IF NOT EXISTS idx_custom_commission_withdrawals_status ON custom_commission_withdrawals(status);
CREATE INDEX IF NOT EXISTS idx_custom_commission_withdrawals_payout_deadline_at ON custom_commission_withdrawals(payout_deadline_at);

CREATE TABLE IF NOT EXISTS custom_commission_withdrawal_items (
    id BIGSERIAL PRIMARY KEY,
    withdrawal_id BIGINT NOT NULL REFERENCES custom_commission_withdrawals(id) ON DELETE CASCADE,
    commission_id BIGINT NOT NULL REFERENCES custom_referral_commissions(id) ON DELETE CASCADE,
    allocated_amount DECIMAL(20,8) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'frozen',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_custom_commission_withdrawal_items_withdrawal_id ON custom_commission_withdrawal_items(withdrawal_id);
CREATE INDEX IF NOT EXISTS idx_custom_commission_withdrawal_items_commission_id ON custom_commission_withdrawal_items(commission_id);

CREATE TABLE IF NOT EXISTS custom_commission_settlement_batches (
    id BIGSERIAL PRIMARY KEY,
    batch_no VARCHAR(64) NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    scanned_count INTEGER NOT NULL DEFAULT 0,
    settled_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    error_summary TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_custom_commission_settlement_batches_status ON custom_commission_settlement_batches(status);
