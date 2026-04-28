-- P3 hardening: referral click risk fields and asset access compatibility.

ALTER TABLE custom_referral_clicks
    ADD COLUMN IF NOT EXISTS ip_hash VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS ua_hash VARCHAR(64) NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_custom_referral_clicks_ip_hash_created_at
    ON custom_referral_clicks(ip_hash, created_at);

CREATE INDEX IF NOT EXISTS idx_custom_referral_clicks_ua_hash_created_at
    ON custom_referral_clicks(ua_hash, created_at);

CREATE INDEX IF NOT EXISTS idx_custom_referral_clicks_invite_code_created_at
    ON custom_referral_clicks(invite_code, created_at);

