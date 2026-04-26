package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

const (
	customInviteCodeLength      = 10
	customInviteCodeMaxAttempts = 12
)

var customInviteCodeCharset = []byte("ABCDEFGHJKLMNPQRSTUVWXYZ23456789")

type customReferralRepository struct {
	client *dbent.Client
	sql    *sql.DB
}

func NewCustomReferralRepository(client *dbent.Client, sqlDB *sql.DB) service.CustomReferralRepository {
	return &customReferralRepository{client: client, sql: sqlDB}
}

func (r *customReferralRepository) UpsertApprovedAffiliate(ctx context.Context, userID, adminID int64, rateOverride *float64) (*service.CustomAffiliate, error) {
	var out *service.CustomAffiliate
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		existing, err := r.getAffiliateByUserIDWithExecutor(txCtx, exec, userID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		now := time.Now()
		if existing == nil {
			code, codeErr := r.generateUniqueInviteCode(txCtx, exec)
			if codeErr != nil {
				return codeErr
			}
			var rate any
			if rateOverride != nil {
				rate = *rateOverride
			}
			if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_affiliates (
    user_id, invite_code, status, source_type, rate_override,
    acquisition_enabled, settlement_enabled, withdrawal_enabled,
    approved_by, approved_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, TRUE, TRUE, TRUE, $6, $7, NOW(), NOW())`,
				userID,
				code,
				service.CustomAffiliateStatusApproved,
				service.CustomAffiliateSourceAdminCreated,
				rate,
				adminIDOrNil(adminID),
				now,
			); err != nil {
				return err
			}
		} else {
			var rate any
			if rateOverride != nil {
				rate = *rateOverride
			}
			if _, err := exec.ExecContext(txCtx, `
UPDATE custom_affiliates
SET status = $2,
    rate_override = COALESCE($3, rate_override),
    acquisition_enabled = TRUE,
    settlement_enabled = TRUE,
    withdrawal_enabled = TRUE,
    risk_reason = '',
    approved_by = $4,
    approved_at = $5,
    disabled_by = NULL,
    disabled_at = NULL,
    updated_at = NOW()
WHERE user_id = $1`,
				userID,
				service.CustomAffiliateStatusApproved,
				rate,
				adminIDOrNil(adminID),
				now,
			); err != nil {
				return err
			}
		}

		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_accounts (affiliate_id, created_at, updated_at)
SELECT id, NOW(), NOW()
FROM custom_affiliates
WHERE user_id = $1
ON CONFLICT (affiliate_id) DO NOTHING`, userID); err != nil {
			return err
		}

		item, err := r.getAffiliateByUserIDWithExecutor(txCtx, exec, userID)
		if err != nil {
			return err
		}
		out = item
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) UpsertAffiliateApplication(ctx context.Context, userID int64, note string) (*service.CustomAffiliate, error) {
	var out *service.CustomAffiliate
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		existing, err := r.getAffiliateByUserIDWithExecutor(txCtx, exec, userID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if existing == nil {
			code, codeErr := r.generateUniqueInviteCode(txCtx, exec)
			if codeErr != nil {
				return codeErr
			}
			if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_affiliates (
    user_id, invite_code, status, source_type,
    acquisition_enabled, settlement_enabled, withdrawal_enabled,
    risk_reason, risk_note, created_at, updated_at
) VALUES ($1, $2, $3, $4, FALSE, FALSE, FALSE, '', $5, NOW(), NOW())`,
				userID,
				code,
				service.CustomAffiliateStatusPending,
				service.CustomAffiliateSourceUserApplied,
				note,
			); err != nil {
				return err
			}
		} else {
			if existing.Status == service.CustomAffiliateStatusApproved {
				return service.ErrCustomReferralAlreadyApproved
			}
			if _, err := exec.ExecContext(txCtx, `
UPDATE custom_affiliates
SET status = $2,
    source_type = $3,
    acquisition_enabled = FALSE,
    settlement_enabled = FALSE,
    withdrawal_enabled = FALSE,
    risk_reason = '',
    risk_note = $4,
    approved_by = NULL,
    approved_at = NULL,
    disabled_by = NULL,
    disabled_at = NULL,
    updated_at = NOW()
WHERE user_id = $1`,
				userID,
				service.CustomAffiliateStatusPending,
				service.CustomAffiliateSourceUserApplied,
				note,
			); err != nil {
				return err
			}
		}

		item, err := r.getAffiliateByUserIDWithExecutor(txCtx, exec, userID)
		if err != nil {
			return err
		}
		out = item
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) SetAffiliateStatus(ctx context.Context, userID, adminID int64, status string, acquisitionEnabled, settlementEnabled, withdrawalEnabled bool, reason string) (*service.CustomAffiliate, error) {
	var out *service.CustomAffiliate
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		res, err := exec.ExecContext(txCtx, `
UPDATE custom_affiliates
SET status = $2,
    acquisition_enabled = $3,
    settlement_enabled = $4,
    withdrawal_enabled = $5,
    risk_reason = $6,
    disabled_by = CASE WHEN $2 = 'disabled' THEN $7 ELSE NULL END,
    disabled_at = CASE WHEN $2 = 'disabled' THEN NOW() ELSE NULL END,
    updated_at = NOW()
WHERE user_id = $1`,
			userID,
			status,
			acquisitionEnabled,
			settlementEnabled,
			withdrawalEnabled,
			strings.TrimSpace(reason),
			adminIDOrNil(adminID),
		)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return service.ErrCustomReferralAffiliateNotFound
		}
		item, err := r.getAffiliateByUserIDWithExecutor(txCtx, exec, userID)
		if err != nil {
			return err
		}
		out = item
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) AdjustAffiliateCommission(ctx context.Context, userID, adminID int64, delta float64, remark string) (*service.CustomAffiliate, error) {
	var out *service.CustomAffiliate
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		affiliate, err := r.getAffiliateByUserIDWithExecutor(txCtx, exec, userID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return service.ErrCustomReferralAffiliateNotFound
			}
			return err
		}

		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_accounts (affiliate_id, created_at, updated_at)
VALUES ($1, NOW(), NOW())
ON CONFLICT (affiliate_id) DO NOTHING`, affiliate.ID); err != nil {
			return err
		}

		res, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_accounts
SET available_amount = available_amount + $2,
    updated_at = NOW()
WHERE affiliate_id = $1
  AND available_amount + $2 >= 0`, affiliate.ID, delta)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return service.ErrCustomReferralAdjustInsufficient
		}

		ledgerType := "commission_adjust_increase"
		if delta < 0 {
			ledgerType = "commission_adjust_decrease"
		}
		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_ledger (
    affiliate_id, type, ref_type, ref_id, external_ref_id,
    delta_available, remark, operator, created_at
) VALUES ($1, $2, 'affiliate', $3, $4, $5, $6, 'admin', NOW())`,
			affiliate.ID,
			ledgerType,
			fmt.Sprintf("%d", affiliate.ID),
			fmt.Sprintf("adjust:%d:%0.8f", affiliate.ID, delta),
			delta,
			strings.TrimSpace(remark),
		); err != nil {
			return err
		}

		out, err = r.getAffiliateByUserIDWithExecutor(txCtx, exec, userID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) GetAffiliateByUserID(ctx context.Context, userID int64) (*service.CustomAffiliate, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, fmt.Errorf("custom referral sql executor is not configured")
	}
	item, err := r.getAffiliateByUserIDWithExecutor(ctx, exec, userID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrCustomReferralAffiliateNotFound
	}
	return item, err
}

func (r *customReferralRepository) getAffiliateByUserIDWithExecutor(ctx context.Context, exec sqlQueryExecutor, userID int64) (*service.CustomAffiliate, error) {
	rows, err := exec.QueryContext(ctx, `
SELECT a.id,
       a.user_id,
       COALESCE(u.email, ''),
       COALESCE(u.username, ''),
       a.invite_code,
       a.status,
       a.source_type,
       a.rate_override::double precision,
       a.acquisition_enabled,
       a.settlement_enabled,
       a.withdrawal_enabled,
       COALESCE(a.risk_reason, ''),
       COALESCE(a.risk_note, ''),
       a.approved_at,
       a.disabled_at
FROM custom_affiliates a
LEFT JOIN users u ON u.id = a.user_id
WHERE a.user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}
	return scanCustomAffiliate(rows)
}

func (r *customReferralRepository) GetApprovedAffiliateByCode(ctx context.Context, code string) (*service.CustomAffiliate, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, fmt.Errorf("custom referral sql executor is not configured")
	}
	rows, err := exec.QueryContext(ctx, `
SELECT a.id,
       a.user_id,
       COALESCE(u.email, ''),
       COALESCE(u.username, ''),
       a.invite_code,
       a.status,
       a.source_type,
       a.rate_override::double precision,
       a.acquisition_enabled,
       a.settlement_enabled,
       a.withdrawal_enabled,
       COALESCE(a.risk_reason, ''),
       COALESCE(a.risk_note, ''),
       a.approved_at,
       a.disabled_at
FROM custom_affiliates a
LEFT JOIN users u ON u.id = a.user_id
WHERE a.invite_code = $1
  AND a.status = $2`, strings.ToUpper(strings.TrimSpace(code)), service.CustomAffiliateStatusApproved)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, service.ErrCustomReferralAffiliateNotFound
	}
	return scanCustomAffiliate(rows)
}

func (r *customReferralRepository) RecordReferralClick(ctx context.Context, affiliateID int64, inviteCode, referer, landingPath string, clickedAt time.Time) error {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return fmt.Errorf("custom referral sql executor is not configured")
	}
	_, err := exec.ExecContext(ctx, `
INSERT INTO custom_referral_clicks (affiliate_id, invite_code, referer, landing_path, created_at)
VALUES ($1, $2, $3, $4, $5)`,
		affiliateID,
		strings.ToUpper(strings.TrimSpace(inviteCode)),
		strings.TrimSpace(referer),
		strings.TrimSpace(landingPath),
		clickedAt,
	)
	return err
}

func (r *customReferralRepository) BindInvitee(ctx context.Context, inviteeUserID, affiliateID, inviterUserID int64, bindSource, bindCode string, boundAt time.Time) (bool, error) {
	var bound bool
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		rows, err := exec.QueryContext(txCtx, `SELECT invitee_user_id FROM custom_referral_bindings WHERE invitee_user_id = $1`, inviteeUserID)
		if err != nil {
			return err
		}
		if rows.Next() {
			_ = rows.Close()
			bound = false
			return nil
		}
		if err := rows.Close(); err != nil {
			return err
		}

		_, err = exec.ExecContext(txCtx, `
INSERT INTO custom_referral_bindings (
    invitee_user_id, inviter_user_id, affiliate_id, bind_source, bind_code, bound_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())`,
			inviteeUserID,
			inviterUserID,
			affiliateID,
			strings.TrimSpace(bindSource),
			strings.ToUpper(strings.TrimSpace(bindCode)),
			boundAt,
		)
		if err != nil {
			if isPQUniqueViolation(err) {
				bound = false
				return nil
			}
			return err
		}
		bound = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return bound, nil
}

func (r *customReferralRepository) CreatePendingCommissionForOrder(ctx context.Context, order service.CustomReferralOrderInput, defaultRate float64, freezeDays int) (float64, error) {
	var applied float64
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		rows, err := exec.QueryContext(txCtx, `
SELECT b.affiliate_id,
       b.inviter_user_id,
       a.rate_override::double precision,
       a.settlement_enabled
FROM custom_referral_bindings b
JOIN custom_affiliates a ON a.id = b.affiliate_id
WHERE b.invitee_user_id = $1`, order.UserID)
		if err != nil {
			return err
		}
		if !rows.Next() {
			_ = rows.Close()
			return nil
		}
		var affiliateID int64
		var inviterUserID int64
		var override sql.NullFloat64
		var settlementEnabled bool
		if err := rows.Scan(&affiliateID, &inviterUserID, &override, &settlementEnabled); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if !settlementEnabled {
			return nil
		}

		rate := defaultRate
		if override.Valid {
			rate = override.Float64
		}
		if rate <= 0 || order.BaseAmount <= 0 {
			return nil
		}
		amount := customRoundTo(order.BaseAmount*(rate/100), 8)
		if amount <= 0 {
			return nil
		}

		settleAt := order.PaidAt
		if settleAt.IsZero() {
			settleAt = time.Now()
		}
		settleAt = settleAt.Add(time.Duration(freezeDays) * 24 * time.Hour)

		insertRows, err := exec.QueryContext(txCtx, `
INSERT INTO custom_referral_commissions (
    affiliate_id, invitee_user_id, order_id, order_type,
    base_amount, rate, commission_amount, refunded_amount, status,
    settle_at, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $9, NOW(), NOW())
ON CONFLICT (order_id) DO NOTHING
RETURNING id`,
			affiliateID,
			order.UserID,
			order.OrderID,
			strings.TrimSpace(order.OrderType),
			order.BaseAmount,
			rate,
			amount,
			service.CustomReferralCommissionStatusPending,
			settleAt,
		)
		if err != nil {
			return err
		}
		if !insertRows.Next() {
			_ = insertRows.Close()
			return nil
		}
		var commissionID int64
		if err := insertRows.Scan(&commissionID); err != nil {
			_ = insertRows.Close()
			return err
		}
		if err := insertRows.Close(); err != nil {
			return err
		}

		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_accounts (affiliate_id, created_at, updated_at)
VALUES ($1, NOW(), NOW())
ON CONFLICT (affiliate_id) DO NOTHING`, affiliateID); err != nil {
			return err
		}
		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_accounts
SET pending_amount = pending_amount + $2,
    updated_at = NOW()
WHERE affiliate_id = $1`, affiliateID, amount); err != nil {
			return err
		}
		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_ledger (
    affiliate_id, commission_id, type, ref_type, ref_id, external_ref_id,
    delta_pending, remark, operator, created_at
) VALUES ($1, $2, 'commission_accrue', 'order', $3, $4, $5, $6, 'system', NOW())`,
			affiliateID,
			commissionID,
			fmt.Sprintf("%d", order.OrderID),
			fmt.Sprintf("order:%d", order.OrderID),
			amount,
			fmt.Sprintf("order_type=%s inviter_user_id=%d", strings.TrimSpace(order.OrderType), inviterUserID),
		); err != nil {
			return err
		}

		applied = amount
		return nil
	})
	if err != nil {
		return 0, err
	}
	return applied, nil
}

func (r *customReferralRepository) ReverseCommissionForRefund(ctx context.Context, refund service.CustomReferralRefundInput) (float64, error) {
	var reversed float64
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		rows, err := exec.QueryContext(txCtx, `
SELECT id,
       affiliate_id,
       base_amount::double precision,
       commission_amount::double precision,
       refunded_amount::double precision,
       status
FROM custom_referral_commissions
WHERE order_id = $1
FOR UPDATE`, refund.OrderID)
		if err != nil {
			return err
		}
		if !rows.Next() {
			_ = rows.Close()
			return nil
		}
		var commissionID int64
		var affiliateID int64
		var baseAmount float64
		var commissionAmount float64
		var refundedAmount float64
		var status string
		if err := rows.Scan(&commissionID, &affiliateID, &baseAmount, &commissionAmount, &refundedAmount, &status); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if baseAmount <= 0 || commissionAmount <= 0 || status == service.CustomReferralCommissionStatusReversed {
			return nil
		}

		target := commissionAmount
		if refund.RefundAmount < baseAmount {
			target = customRoundTo(commissionAmount*(refund.RefundAmount/baseAmount), 8)
		}
		reverseAmount := customRoundTo(target-refundedAmount, 8)
		if reverseAmount <= 0 {
			return nil
		}
		if remaining := customRoundTo(commissionAmount-refundedAmount, 8); reverseAmount > remaining {
			reverseAmount = remaining
		}
		if reverseAmount <= 0 {
			return nil
		}

		nextRefunded := customRoundTo(refundedAmount+reverseAmount, 8)
		nextStatus := status
		reversedAt := any(nil)
		reversedReason := ""
		if nearlyEqual(nextRefunded, commissionAmount) || nextRefunded > commissionAmount {
			nextStatus = service.CustomReferralCommissionStatusReversed
			reversedAt = refund.RefundedAt
			if refund.RefundedAt.IsZero() {
				reversedAt = time.Now()
			}
			reversedReason = strings.TrimSpace(refund.Reason)
		}

		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_referral_commissions
SET refunded_amount = $2,
    status = $3,
    reversed_at = CASE WHEN $4::timestamptz IS NULL THEN reversed_at ELSE $4::timestamptz END,
    reversed_reason = CASE WHEN $5 = '' THEN reversed_reason ELSE $5 END,
    updated_at = NOW()
WHERE id = $1`,
			commissionID,
			nextRefunded,
			nextStatus,
			reversedAt,
			reversedReason,
		); err != nil {
			return err
		}

		accountField := "pending_amount"
		deltaPending := reverseAmount
		deltaAvailable := 0.0
		if status == service.CustomReferralCommissionStatusAvailable {
			accountField = "available_amount"
			deltaPending = 0
			deltaAvailable = reverseAmount
		}

		if _, err := exec.ExecContext(txCtx, fmt.Sprintf(`
UPDATE custom_commission_accounts
SET %s = %s - $2,
    reversed_amount = reversed_amount + $2,
    updated_at = NOW()
WHERE affiliate_id = $1`, accountField, accountField), affiliateID, reverseAmount); err != nil {
			return err
		}

		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_ledger (
    affiliate_id, commission_id, type, ref_type, ref_id, external_ref_id,
    delta_pending, delta_available, delta_reversed, remark, operator, created_at
) VALUES ($1, $2, 'commission_reverse', 'refund', $3, $4, $5, $6, $7, $8, 'system', NOW())`,
			affiliateID,
			commissionID,
			fmt.Sprintf("%d", refund.OrderID),
			fmt.Sprintf("refund:%d:%0.8f", refund.OrderID, reverseAmount),
			-deltaPending,
			-deltaAvailable,
			reverseAmount,
			strings.TrimSpace(refund.Reason),
		); err != nil {
			return err
		}

		reversed = reverseAmount
		return nil
	})
	if err != nil {
		return 0, err
	}
	return reversed, nil
}

func (r *customReferralRepository) GetDashboardByUserID(ctx context.Context, userID int64) (*service.CustomReferralDashboard, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, fmt.Errorf("custom referral sql executor is not configured")
	}
	rows, err := exec.QueryContext(ctx, `
SELECT a.status,
       a.invite_code,
       COALESCE(a.rate_override::double precision, NULL),
       COALESCE(a.acquisition_enabled, FALSE),
       COALESCE(a.settlement_enabled, FALSE),
       COALESCE(a.withdrawal_enabled, FALSE),
       COALESCE(clicks.cnt, 0),
       COALESCE(bindings.cnt, 0),
       COALESCE(paid.cnt, 0),
       COALESCE(acc.pending_amount::double precision, 0),
       COALESCE(acc.available_amount::double precision, 0),
       COALESCE(acc.frozen_amount::double precision, 0),
       COALESCE(acc.withdrawn_amount::double precision, 0),
       COALESCE(acc.reversed_amount::double precision, 0),
       COALESCE(acc.debt_amount::double precision, 0)
FROM custom_affiliates a
LEFT JOIN custom_commission_accounts acc ON acc.affiliate_id = a.id
LEFT JOIN (
    SELECT affiliate_id, COUNT(*) AS cnt
    FROM custom_referral_clicks
    GROUP BY affiliate_id
) clicks ON clicks.affiliate_id = a.id
LEFT JOIN (
    SELECT affiliate_id, COUNT(*) AS cnt
    FROM custom_referral_bindings
    GROUP BY affiliate_id
) bindings ON bindings.affiliate_id = a.id
LEFT JOIN (
    SELECT affiliate_id, COUNT(DISTINCT invitee_user_id) AS cnt
    FROM custom_referral_commissions
    WHERE commission_amount > 0
    GROUP BY affiliate_id
) paid ON paid.affiliate_id = a.id
WHERE a.user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var out service.CustomReferralDashboard
	var rate sql.NullFloat64
	if err := rows.Scan(
		&out.Status,
		&out.InviteCode,
		&rate,
		&out.AcquisitionEnabled,
		&out.SettlementEnabled,
		&out.WithdrawalEnabled,
		&out.ClickCount,
		&out.BoundUserCount,
		&out.PaidUserCount,
		&out.PendingAmount,
		&out.AvailableAmount,
		&out.FrozenAmount,
		&out.WithdrawnAmount,
		&out.ReversedAmount,
		&out.DebtAmount,
	); err != nil {
		return nil, err
	}
	if rate.Valid {
		out.Rate = &rate.Float64
	}
	return &out, nil
}

func (r *customReferralRepository) ListAffiliates(ctx context.Context, params service.CustomReferralListParams) ([]service.CustomAffiliate, int64, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, 0, fmt.Errorf("custom referral sql executor is not configured")
	}
	offset := (params.Page - 1) * params.PageSize
	keyword := "%" + strings.TrimSpace(params.Keyword) + "%"

	countRows, err := exec.QueryContext(ctx, `
SELECT COUNT(*)
FROM custom_affiliates a
LEFT JOIN users u ON u.id = a.user_id
WHERE ($1 = '' OR a.status = $1)
  AND ($2 = '%%' OR COALESCE(u.email, '') ILIKE $2 OR COALESCE(u.username, '') ILIKE $2 OR a.invite_code ILIKE $2)`,
		strings.TrimSpace(params.Status),
		keyword,
	)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if countRows.Next() {
		if err := countRows.Scan(&total); err != nil {
			_ = countRows.Close()
			return nil, 0, err
		}
	}
	if err := countRows.Close(); err != nil {
		return nil, 0, err
	}

	rows, err := exec.QueryContext(ctx, `
SELECT a.id,
       a.user_id,
       COALESCE(u.email, ''),
       COALESCE(u.username, ''),
       a.invite_code,
       a.status,
       a.source_type,
       a.rate_override::double precision,
       COALESCE(clicks.cnt, 0),
       COALESCE(bindings.cnt, 0),
       COALESCE(paid.cnt, 0),
       COALESCE(acc.pending_amount::double precision, 0),
       COALESCE(acc.available_amount::double precision, 0),
       COALESCE(acc.withdrawn_amount::double precision, 0),
       a.acquisition_enabled,
       a.settlement_enabled,
       a.withdrawal_enabled,
       COALESCE(a.risk_reason, ''),
       COALESCE(a.risk_note, ''),
       a.approved_at,
       a.disabled_at
FROM custom_affiliates a
LEFT JOIN users u ON u.id = a.user_id
LEFT JOIN custom_commission_accounts acc ON acc.affiliate_id = a.id
LEFT JOIN (
    SELECT affiliate_id, COUNT(*) AS cnt
    FROM custom_referral_clicks
    GROUP BY affiliate_id
) clicks ON clicks.affiliate_id = a.id
LEFT JOIN (
    SELECT affiliate_id, COUNT(*) AS cnt
    FROM custom_referral_bindings
    GROUP BY affiliate_id
) bindings ON bindings.affiliate_id = a.id
LEFT JOIN (
    SELECT affiliate_id, COUNT(DISTINCT invitee_user_id) AS cnt
    FROM custom_referral_commissions
    WHERE commission_amount > 0
    GROUP BY affiliate_id
) paid ON paid.affiliate_id = a.id
WHERE ($1 = '' OR a.status = $1)
  AND ($2 = '%%' OR COALESCE(u.email, '') ILIKE $2 OR COALESCE(u.username, '') ILIKE $2 OR a.invite_code ILIKE $2)
ORDER BY a.updated_at DESC, a.id DESC
LIMIT $3 OFFSET $4`,
		strings.TrimSpace(params.Status),
		keyword,
		params.PageSize,
		offset,
	)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	items := make([]service.CustomAffiliate, 0)
	for rows.Next() {
		item, err := scanCustomAffiliateWithStats(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (r *customReferralRepository) ListAffiliateBindings(ctx context.Context, affiliateUserID int64, page, pageSize int) ([]service.CustomReferralBindingDetail, int64, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, 0, fmt.Errorf("custom referral sql executor is not configured")
	}
	offset := (page - 1) * pageSize

	countRows, err := exec.QueryContext(ctx, `
SELECT COUNT(*)
FROM custom_referral_bindings b
JOIN custom_affiliates a ON a.id = b.affiliate_id
WHERE a.user_id = $1`, affiliateUserID)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if countRows.Next() {
		if err := countRows.Scan(&total); err != nil {
			_ = countRows.Close()
			return nil, 0, err
		}
	}
	if err := countRows.Close(); err != nil {
		return nil, 0, err
	}

	rows, err := exec.QueryContext(ctx, `
SELECT b.id,
       b.invitee_user_id,
       COALESCE(u.email, ''),
       COALESCE(u.username, ''),
       b.bound_at
FROM custom_referral_bindings b
JOIN custom_affiliates a ON a.id = b.affiliate_id
LEFT JOIN users u ON u.id = b.invitee_user_id
WHERE a.user_id = $1
ORDER BY b.bound_at DESC, b.id DESC
LIMIT $2 OFFSET $3`, affiliateUserID, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

	items := make([]service.CustomReferralBindingDetail, 0)
	for rows.Next() {
		var item service.CustomReferralBindingDetail
		if err := rows.Scan(
			&item.ID,
			&item.InviteeUserID,
			&item.InviteeEmail,
			&item.InviteeName,
			&item.BoundAt,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (r *customReferralRepository) GetAdminOverview(ctx context.Context) (*service.CustomReferralAdminOverview, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, fmt.Errorf("custom referral sql executor is not configured")
	}
	rows, err := exec.QueryContext(ctx, `
SELECT
    COALESCE((SELECT COUNT(*) FROM custom_affiliates), 0),
    COALESCE((SELECT COUNT(*) FROM custom_affiliates WHERE status = 'approved'), 0),
    COALESCE((SELECT COUNT(*) FROM custom_affiliates WHERE status = 'disabled'), 0),
    COALESCE((SELECT SUM(pending_amount)::double precision FROM custom_commission_accounts), 0),
    COALESCE((SELECT SUM(available_amount)::double precision FROM custom_commission_accounts), 0),
    COALESCE((SELECT SUM(frozen_amount)::double precision FROM custom_commission_accounts), 0),
    COALESCE((SELECT SUM(withdrawn_amount)::double precision FROM custom_commission_accounts), 0),
    COALESCE((SELECT COUNT(*) FROM custom_referral_clicks), 0),
    COALESCE((SELECT COUNT(*) FROM custom_referral_bindings), 0),
    COALESCE((SELECT COUNT(DISTINCT invitee_user_id) FROM custom_referral_commissions WHERE commission_amount > 0), 0)`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return &service.CustomReferralAdminOverview{}, nil
	}
	var out service.CustomReferralAdminOverview
	if err := rows.Scan(
		&out.TotalAffiliates,
		&out.ApprovedAffiliates,
		&out.DisabledAffiliates,
		&out.PendingAmount,
		&out.AvailableAmount,
		&out.FrozenAmount,
		&out.WithdrawnAmount,
		&out.ReferralClickCount,
		&out.BoundUserCount,
		&out.EffectivePaidUserCount,
	); err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *customReferralRepository) ListCommissionsByUserID(ctx context.Context, userID int64, params service.CustomReferralCommissionListParams) ([]service.CustomReferralCommission, int64, error) {
	return r.listCommissions(ctx, params, "WHERE a.user_id = $1 AND ($2 = '' OR c.status = $2)", userID)
}

func (r *customReferralRepository) ListAffiliateCommissions(ctx context.Context, affiliateUserID int64, params service.CustomReferralCommissionListParams) ([]service.CustomReferralCommission, int64, error) {
	return r.listCommissions(ctx, params, "WHERE a.user_id = $1 AND ($2 = '' OR c.status = $2)", affiliateUserID)
}

func (r *customReferralRepository) ListCommissions(ctx context.Context, params service.CustomReferralCommissionListParams) ([]service.CustomReferralCommission, int64, error) {
	return r.listCommissions(ctx, params, "WHERE ($1 = '' OR c.status = $1)")
}

func (r *customReferralRepository) listCommissions(ctx context.Context, params service.CustomReferralCommissionListParams, where string, args ...any) ([]service.CustomReferralCommission, int64, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, 0, fmt.Errorf("custom referral sql executor is not configured")
	}
	offset := (params.Page - 1) * params.PageSize
	status := strings.TrimSpace(params.Status)
	countSQL := `
SELECT COUNT(*)
FROM custom_referral_commissions c
JOIN custom_affiliates a ON a.id = c.affiliate_id
` + where
	listSQL := `
SELECT c.id,
       c.affiliate_id,
       a.user_id,
       COALESCE(au.email, ''),
       c.order_id,
       c.invitee_user_id,
       COALESCE(iu.email, ''),
       COALESCE(iu.username, ''),
       c.order_type,
       c.base_amount::double precision,
       c.rate::double precision,
       c.commission_amount::double precision,
       c.refunded_amount::double precision,
       c.status,
       c.settle_at,
       c.available_at,
       c.reversed_at,
       COALESCE(c.reversed_reason, ''),
       c.created_at
FROM custom_referral_commissions c
JOIN custom_affiliates a ON a.id = c.affiliate_id
LEFT JOIN users au ON au.id = a.user_id
LEFT JOIN users iu ON iu.id = c.invitee_user_id
` + where + `
ORDER BY c.created_at DESC, c.id DESC`

	countArgs := append([]any{}, args...)
	if strings.Contains(where, "$2") {
		countArgs = append(countArgs, status)
	} else {
		countArgs = []any{status}
	}
	countRows, err := exec.QueryContext(ctx, countSQL, countArgs...)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if countRows.Next() {
		if err := countRows.Scan(&total); err != nil {
			_ = countRows.Close()
			return nil, 0, err
		}
	}
	if err := countRows.Close(); err != nil {
		return nil, 0, err
	}

	listArgs := append([]any{}, args...)
	if strings.Contains(where, "$2") {
		listSQL += `
LIMIT $3 OFFSET $4`
		listArgs = append(listArgs, status, params.PageSize, offset)
	} else {
		listSQL += `
LIMIT $2 OFFSET $3`
		listArgs = []any{status, params.PageSize, offset}
	}

	rows, err := exec.QueryContext(ctx, listSQL, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	items := make([]service.CustomReferralCommission, 0)
	for rows.Next() {
		item, err := scanCustomReferralCommission(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (r *customReferralRepository) RunSettlementBatch(ctx context.Context, now time.Time) (*service.CustomReferralSettlementBatch, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, fmt.Errorf("custom referral sql executor is not configured")
	}
	batchNo, err := generateSettlementBatchNo()
	if err != nil {
		return nil, err
	}
	row, err := exec.QueryContext(ctx, `
INSERT INTO custom_commission_settlement_batches (batch_no, status, started_at, finished_at, scanned_count, settled_count, skipped_count, failed_count, error_summary)
VALUES ($1, 'running', $2, NULL, 0, 0, 0, 0, '')
RETURNING id`, batchNo, now)
	if err != nil {
		return nil, err
	}
	var batchID int64
	if !row.Next() {
		_ = row.Close()
		return nil, fmt.Errorf("failed to create settlement batch")
	}
	if err := row.Scan(&batchID); err != nil {
		_ = row.Close()
		return nil, err
	}
	if err := row.Close(); err != nil {
		return nil, err
	}

	result := &service.CustomReferralSettlementBatch{
		ID:        batchID,
		BatchNo:   batchNo,
		Status:    "running",
		StartedAt: now,
	}

	processErr := r.withTx(ctx, func(txCtx context.Context, txExec sqlQueryExecutor) error {
		return r.settleDueCommissions(txCtx, txExec, now, result)
	})
	if processErr != nil {
		result.Status = "failed"
		result.ErrorSummary = processErr.Error()
		result.FinishedAt = &now
		result.FailedCount = result.ScannedCount - result.SettledCount - result.SkippedCount
		_, _ = exec.ExecContext(ctx, `
UPDATE custom_commission_settlement_batches
SET status = $2,
    finished_at = $3,
    scanned_count = $4,
    settled_count = $5,
    skipped_count = $6,
    failed_count = $7,
    error_summary = $8
WHERE id = $1`,
			batchID,
			result.Status,
			now,
			result.ScannedCount,
			result.SettledCount,
			result.SkippedCount,
			result.FailedCount,
			result.ErrorSummary,
		)
		return result, processErr
	}

	result.Status = "completed"
	result.FinishedAt = &now
	_, err = exec.ExecContext(ctx, `
UPDATE custom_commission_settlement_batches
SET status = $2,
    finished_at = $3,
    scanned_count = $4,
    settled_count = $5,
    skipped_count = $6,
    failed_count = $7,
    error_summary = $8
WHERE id = $1`,
		batchID,
		result.Status,
		now,
		result.ScannedCount,
		result.SettledCount,
		result.SkippedCount,
		result.FailedCount,
		result.ErrorSummary,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *customReferralRepository) SettleDueCommissions(ctx context.Context, now time.Time) error {
	return r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		return r.settleDueCommissions(txCtx, exec, now, nil)
	})
}

func (r *customReferralRepository) settleDueCommissions(ctx context.Context, exec sqlQueryExecutor, now time.Time, result *service.CustomReferralSettlementBatch) error {
	rows, err := exec.QueryContext(ctx, `
SELECT c.id,
       c.affiliate_id,
       c.commission_amount::double precision,
       a.settlement_enabled
FROM custom_referral_commissions c
JOIN custom_affiliates a ON a.id = c.affiliate_id
WHERE c.status = $1
  AND c.settle_at <= $2
ORDER BY c.settle_at ASC, c.id ASC
FOR UPDATE`, service.CustomReferralCommissionStatusPending, now)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var commissionID int64
		var affiliateID int64
		var amount float64
		var settlementEnabled bool
		if err := rows.Scan(&commissionID, &affiliateID, &amount, &settlementEnabled); err != nil {
			return err
		}
		if result != nil {
			result.ScannedCount++
		}
		if !settlementEnabled || amount <= 0 {
			if result != nil {
				result.SkippedCount++
			}
			continue
		}
		if _, err := exec.ExecContext(ctx, `
UPDATE custom_referral_commissions
SET status = $2,
    available_at = $3,
    updated_at = NOW()
WHERE id = $1
  AND status = $4`,
			commissionID,
			service.CustomReferralCommissionStatusAvailable,
			now,
			service.CustomReferralCommissionStatusPending,
		); err != nil {
			return err
		}
		if _, err := exec.ExecContext(ctx, `
UPDATE custom_commission_accounts
SET pending_amount = pending_amount - $2,
    available_amount = available_amount + $2,
    updated_at = NOW()
WHERE affiliate_id = $1`, affiliateID, amount); err != nil {
			return err
		}
		if _, err := exec.ExecContext(ctx, `
INSERT INTO custom_commission_ledger (
    affiliate_id, commission_id, type, ref_type, ref_id, external_ref_id,
    delta_pending, delta_available, remark, operator, created_at
) VALUES ($1, $2, 'commission_settle', 'commission', $3, $4, $5, $6, '', 'system', NOW())`,
			affiliateID,
			commissionID,
			fmt.Sprintf("%d", commissionID),
			fmt.Sprintf("settle:%d", commissionID),
			-amount,
			amount,
		); err != nil {
			return err
		}
		if result != nil {
			result.SettledCount++
		}
	}
	return rows.Err()
}

func (r *customReferralRepository) CreateWithdrawal(ctx context.Context, input service.CustomReferralWithdrawalCreateInput, feeAmount float64) (*service.CustomReferralWithdrawal, error) {
	var out *service.CustomReferralWithdrawal
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		affiliate, err := r.getAffiliateByUserIDWithExecutor(txCtx, exec, input.UserID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return service.ErrCustomReferralPermissionDenied
			}
			return err
		}
		if affiliate.Status != service.CustomAffiliateStatusApproved || !affiliate.WithdrawalEnabled {
			return service.ErrCustomReferralWithdrawDisabled
		}

		accountRows, err := exec.QueryContext(txCtx, `
SELECT available_amount::double precision
FROM custom_commission_accounts
WHERE affiliate_id = $1
FOR UPDATE`, affiliate.ID)
		if err != nil {
			return err
		}
		availableAmount := 0.0
		if accountRows.Next() {
			if err := accountRows.Scan(&availableAmount); err != nil {
				_ = accountRows.Close()
				return err
			}
		}
		if err := accountRows.Close(); err != nil {
			return err
		}
		if customRoundTo(availableAmount, 8) < customRoundTo(input.Amount, 8) {
			return service.ErrCustomReferralWithdrawInsufficient
		}

		allocRows, err := exec.QueryContext(txCtx, `
SELECT c.id,
       (c.commission_amount::double precision - c.refunded_amount::double precision - COALESCE((
           SELECT SUM(wi.allocated_amount)::double precision
           FROM custom_commission_withdrawal_items wi
           WHERE wi.commission_id = c.id
             AND wi.status IN ('frozen', 'withdrawn')
       ), 0)) AS remaining
FROM custom_referral_commissions c
WHERE c.affiliate_id = $1
  AND c.status = $2
ORDER BY COALESCE(c.available_at, c.created_at) ASC, c.id ASC
FOR UPDATE`, affiliate.ID, service.CustomReferralCommissionStatusAvailable)
		if err != nil {
			return err
		}
		defer func() { _ = allocRows.Close() }()

		type allocation struct {
			commissionID int64
			amount       float64
		}
		remaining := input.Amount
		allocations := make([]allocation, 0)
		for allocRows.Next() && remaining > 0 {
			var commissionID int64
			var available float64
			if err := allocRows.Scan(&commissionID, &available); err != nil {
				return err
			}
			if available <= 0 {
				continue
			}
			useAmount := math.Min(available, remaining)
			allocations = append(allocations, allocation{commissionID: commissionID, amount: customRoundTo(useAmount, 8)})
			remaining = customRoundTo(remaining-useAmount, 8)
		}
		if err := allocRows.Err(); err != nil {
			return err
		}
		// Manual commission adjustments are currently reflected directly in
		// custom_commission_accounts.available_amount and ledger records rather
		// than custom_referral_commissions rows. As long as the locked account
		// balance covers the requested amount, we allow the unmatched remainder
		// to flow through this withdrawal without allocating extra
		// custom_commission_withdrawal_items rows.

		netAmount := customRoundTo(input.Amount-feeAmount, 8)
		if netAmount <= 0 {
			return service.ErrCustomReferralWithdrawTooSmall
		}

		now := time.Now()
		withdrawRows, err := exec.QueryContext(txCtx, `
INSERT INTO custom_commission_withdrawals (
    affiliate_id, amount, fee_amount, net_amount, account_type, account_name, account_no, account_network,
    qr_image_url, contact_info, applicant_note, status, submitted_at, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())
RETURNING id`,
			affiliate.ID,
			input.Amount,
			feeAmount,
			netAmount,
			strings.TrimSpace(input.AccountType),
			strings.TrimSpace(input.AccountName),
			strings.TrimSpace(input.AccountNo),
			strings.TrimSpace(input.AccountNetwork),
			strings.TrimSpace(input.QRImageURL),
			strings.TrimSpace(input.ContactInfo),
			strings.TrimSpace(input.ApplicantNote),
			service.CustomReferralWithdrawalStatusPending,
			now,
		)
		if err != nil {
			return err
		}
		var withdrawalID int64
		if !withdrawRows.Next() {
			_ = withdrawRows.Close()
			return fmt.Errorf("failed to create withdrawal")
		}
		if err := withdrawRows.Scan(&withdrawalID); err != nil {
			_ = withdrawRows.Close()
			return err
		}
		if err := withdrawRows.Close(); err != nil {
			return err
		}

		for _, item := range allocations {
			if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_withdrawal_items (withdrawal_id, commission_id, allocated_amount, status, created_at)
VALUES ($1, $2, $3, $4, NOW())`,
				withdrawalID,
				item.commissionID,
				item.amount,
				service.CustomReferralWithdrawalItemStatusFrozen,
			); err != nil {
				return err
			}
		}

		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_accounts
SET available_amount = available_amount - $2,
    frozen_amount = frozen_amount + $2,
    updated_at = NOW()
WHERE affiliate_id = $1`, affiliate.ID, input.Amount); err != nil {
			return err
		}
		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_ledger (
    affiliate_id, withdrawal_id, type, ref_type, ref_id, external_ref_id,
    delta_available, delta_frozen, remark, operator, created_at
) VALUES ($1, $2, 'withdrawal_apply', 'withdrawal', $3, $4, $5, $6, $7, 'user', NOW())`,
			affiliate.ID,
			withdrawalID,
			fmt.Sprintf("%d", withdrawalID),
			fmt.Sprintf("withdrawal:%d", withdrawalID),
			-input.Amount,
			input.Amount,
			strings.TrimSpace(input.ApplicantNote),
		); err != nil {
			return err
		}

		withdrawal, err := r.getWithdrawalByID(txCtx, exec, withdrawalID)
		if err != nil {
			return err
		}
		out = withdrawal
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) ListWithdrawalsByUserID(ctx context.Context, userID int64, params service.CustomReferralWithdrawalListParams) ([]service.CustomReferralWithdrawal, int64, error) {
	return r.listWithdrawals(ctx, params, "WHERE a.user_id = $1 AND ($2 = '' OR w.status = $2)", userID)
}

func (r *customReferralRepository) ListAffiliateWithdrawals(ctx context.Context, affiliateUserID int64, params service.CustomReferralWithdrawalListParams) ([]service.CustomReferralWithdrawal, int64, error) {
	return r.listWithdrawals(ctx, params, "WHERE a.user_id = $1 AND ($2 = '' OR w.status = $2)", affiliateUserID)
}

func (r *customReferralRepository) ListWithdrawals(ctx context.Context, params service.CustomReferralWithdrawalListParams) ([]service.CustomReferralWithdrawal, int64, error) {
	return r.listWithdrawals(ctx, params, "WHERE ($1 = '' OR w.status = $1)")
}

func (r *customReferralRepository) listWithdrawals(ctx context.Context, params service.CustomReferralWithdrawalListParams, where string, args ...any) ([]service.CustomReferralWithdrawal, int64, error) {
	exec := txAwareSQLExecutor(ctx, r.sql, r.client)
	if exec == nil {
		return nil, 0, fmt.Errorf("custom referral sql executor is not configured")
	}
	offset := (params.Page - 1) * params.PageSize
	status := strings.TrimSpace(params.Status)
	countSQL := `
SELECT COUNT(*)
FROM custom_commission_withdrawals w
JOIN custom_affiliates a ON a.id = w.affiliate_id
LEFT JOIN users u ON u.id = a.user_id
` + where
	listSQL := `
SELECT w.id,
       w.affiliate_id,
       a.user_id,
       COALESCE(u.email, ''),
       a.invite_code,
       w.amount::double precision,
       w.fee_amount::double precision,
       w.net_amount::double precision,
       w.account_type,
       COALESCE(w.account_name, ''),
       COALESCE(w.account_no, ''),
       COALESCE(w.account_network, ''),
       COALESCE(w.qr_image_url, ''),
       COALESCE(w.contact_info, ''),
       COALESCE(w.applicant_note, ''),
       COALESCE(w.admin_note, ''),
       COALESCE(w.payment_proof_url, ''),
       COALESCE(w.payment_txn_no, ''),
       w.status,
       w.submitted_at,
       w.approved_at,
       w.payout_deadline_at,
       w.paid_at,
       w.rejected_at,
       w.canceled_at,
       COALESCE(w.reject_reason, '')
FROM custom_commission_withdrawals w
JOIN custom_affiliates a ON a.id = w.affiliate_id
LEFT JOIN users u ON u.id = a.user_id
` + where + `
ORDER BY w.submitted_at DESC, w.id DESC`

	countArgs := append([]any{}, args...)
	if strings.Contains(where, "$2") {
		countArgs = append(countArgs, status)
	} else {
		countArgs = []any{status}
	}
	countRows, err := exec.QueryContext(ctx, countSQL, countArgs...)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if countRows.Next() {
		if err := countRows.Scan(&total); err != nil {
			_ = countRows.Close()
			return nil, 0, err
		}
	}
	if err := countRows.Close(); err != nil {
		return nil, 0, err
	}

	listArgs := append([]any{}, args...)
	if strings.Contains(where, "$2") {
		listSQL += `
LIMIT $3 OFFSET $4`
		listArgs = append(listArgs, status, params.PageSize, offset)
	} else {
		listSQL += `
LIMIT $2 OFFSET $3`
		listArgs = []any{status, params.PageSize, offset}
	}
	rows, err := exec.QueryContext(ctx, listSQL, listArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	items := make([]service.CustomReferralWithdrawal, 0)
	for rows.Next() {
		item, err := scanCustomReferralWithdrawal(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (r *customReferralRepository) CancelWithdrawal(ctx context.Context, withdrawalID, userID int64) (*service.CustomReferralWithdrawal, error) {
	return r.releaseWithdrawal(ctx, withdrawalID, userID, "user", service.CustomReferralWithdrawalStatusCanceled, "", true)
}

func (r *customReferralRepository) ApproveWithdrawal(ctx context.Context, input service.CustomReferralWithdrawalReviewInput, deadlineAt time.Time) (*service.CustomReferralWithdrawal, error) {
	var out *service.CustomReferralWithdrawal
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		item, err := r.getWithdrawalByID(txCtx, exec, input.WithdrawalID)
		if err != nil {
			return err
		}
		if item.Status != service.CustomReferralWithdrawalStatusPending {
			return service.ErrCustomReferralWithdrawalNotFound
		}
		now := time.Now()
		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_withdrawals
SET status = $2,
    approved_at = $3,
    payout_deadline_at = $4,
    reviewed_by = $5,
    admin_note = $6,
    updated_at = NOW()
WHERE id = $1`,
			input.WithdrawalID,
			service.CustomReferralWithdrawalStatusApproved,
			now,
			deadlineAt,
			adminIDOrNil(input.AdminUserID),
			strings.TrimSpace(input.AdminNote),
		); err != nil {
			return err
		}
		out, err = r.getWithdrawalByID(txCtx, exec, input.WithdrawalID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) RejectWithdrawal(ctx context.Context, input service.CustomReferralWithdrawalReviewInput) (*service.CustomReferralWithdrawal, error) {
	return r.releaseWithdrawal(ctx, input.WithdrawalID, input.AdminUserID, "admin", service.CustomReferralWithdrawalStatusRejected, input.RejectReason, false)
}

func (r *customReferralRepository) MarkWithdrawalPaid(ctx context.Context, input service.CustomReferralWithdrawalPayInput) (*service.CustomReferralWithdrawal, error) {
	var out *service.CustomReferralWithdrawal
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		item, err := r.getWithdrawalByID(txCtx, exec, input.WithdrawalID)
		if err != nil {
			return err
		}
		if item.Status != service.CustomReferralWithdrawalStatusApproved {
			return service.ErrCustomReferralWithdrawalNotFound
		}
		now := time.Now()
		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_withdrawals
SET status = $2,
    paid_at = $3,
    reviewed_by = $4,
    admin_note = $5,
    payment_proof_url = $6,
    payment_txn_no = $7,
    updated_at = NOW()
WHERE id = $1`,
			input.WithdrawalID,
			service.CustomReferralWithdrawalStatusPaid,
			now,
			adminIDOrNil(input.AdminUserID),
			strings.TrimSpace(input.AdminNote),
			strings.TrimSpace(input.PaymentProofURL),
			strings.TrimSpace(input.PaymentTxnNo),
		); err != nil {
			return err
		}
		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_accounts
SET frozen_amount = frozen_amount - $2,
    withdrawn_amount = withdrawn_amount + $2,
    updated_at = NOW()
WHERE affiliate_id = $1`, item.AffiliateID, item.Amount); err != nil {
			return err
		}
		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_withdrawal_items
SET status = $2
WHERE withdrawal_id = $1
  AND status = $3`,
			input.WithdrawalID,
			service.CustomReferralWithdrawalItemStatusWithdrawn,
			service.CustomReferralWithdrawalItemStatusFrozen,
		); err != nil {
			return err
		}
		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_ledger (
    affiliate_id, withdrawal_id, type, ref_type, ref_id, external_ref_id,
    delta_frozen, delta_withdrawn, remark, operator, created_at
) VALUES ($1, $2, 'withdrawal_paid', 'withdrawal', $3, $4, $5, $6, $7, 'admin', NOW())`,
			item.AffiliateID,
			item.ID,
			fmt.Sprintf("%d", item.ID),
			fmt.Sprintf("withdrawal:%d", item.ID),
			-item.Amount,
			item.Amount,
			strings.TrimSpace(input.AdminNote),
		); err != nil {
			return err
		}
		out, err = r.getWithdrawalByID(txCtx, exec, input.WithdrawalID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) releaseWithdrawal(ctx context.Context, withdrawalID, actorUserID int64, operator, targetStatus, reason string, actorMustOwn bool) (*service.CustomReferralWithdrawal, error) {
	var out *service.CustomReferralWithdrawal
	err := r.withTx(ctx, func(txCtx context.Context, exec sqlQueryExecutor) error {
		item, err := r.getWithdrawalByID(txCtx, exec, withdrawalID)
		if err != nil {
			return err
		}
		if actorMustOwn && item.AffiliateUserID != actorUserID {
			return service.ErrCustomReferralWithdrawalNotFound
		}
		if targetStatus == service.CustomReferralWithdrawalStatusCanceled {
			if item.Status != service.CustomReferralWithdrawalStatusPending {
				return service.ErrCustomReferralWithdrawalNotFound
			}
		} else if item.Status != service.CustomReferralWithdrawalStatusPending && item.Status != service.CustomReferralWithdrawalStatusApproved {
			return service.ErrCustomReferralWithdrawalNotFound
		}
		now := time.Now()
		switch targetStatus {
		case service.CustomReferralWithdrawalStatusCanceled:
			if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_withdrawals
SET status = $2,
    canceled_at = $3,
    canceled_by = $4,
    updated_at = NOW()
WHERE id = $1`,
				withdrawalID,
				targetStatus,
				now,
				adminIDOrNil(actorUserID),
			); err != nil {
				return err
			}
		case service.CustomReferralWithdrawalStatusRejected:
			if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_withdrawals
SET status = $2,
    rejected_at = $3,
    reviewed_by = $4,
    reject_reason = $5,
    admin_note = $6,
    updated_at = NOW()
WHERE id = $1`,
				withdrawalID,
				targetStatus,
				now,
				adminIDOrNil(actorUserID),
				strings.TrimSpace(reason),
				strings.TrimSpace(reason),
			); err != nil {
				return err
			}
		default:
			return service.ErrCustomReferralWithdrawalNotFound
		}

		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_accounts
SET frozen_amount = frozen_amount - $2,
    available_amount = available_amount + $2,
    updated_at = NOW()
WHERE affiliate_id = $1`, item.AffiliateID, item.Amount); err != nil {
			return err
		}
		if _, err := exec.ExecContext(txCtx, `
UPDATE custom_commission_withdrawal_items
SET status = $2
WHERE withdrawal_id = $1
  AND status = $3`,
			withdrawalID,
			service.CustomReferralWithdrawalItemStatusReleased,
			service.CustomReferralWithdrawalItemStatusFrozen,
		); err != nil {
			return err
		}
		ledgerType := "withdrawal_cancel"
		if targetStatus == service.CustomReferralWithdrawalStatusRejected {
			ledgerType = "withdrawal_reject"
		}
		if _, err := exec.ExecContext(txCtx, `
INSERT INTO custom_commission_ledger (
    affiliate_id, withdrawal_id, type, ref_type, ref_id, external_ref_id,
    delta_available, delta_frozen, remark, operator, created_at
) VALUES ($1, $2, $3, 'withdrawal', $4, $5, $6, $7, $8, $9, NOW())`,
			item.AffiliateID,
			item.ID,
			ledgerType,
			fmt.Sprintf("%d", item.ID),
			fmt.Sprintf("withdrawal:%d", item.ID),
			item.Amount,
			-item.Amount,
			strings.TrimSpace(reason),
			operator,
		); err != nil {
			return err
		}
		out, err = r.getWithdrawalByID(txCtx, exec, withdrawalID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *customReferralRepository) getWithdrawalByID(ctx context.Context, exec sqlQueryExecutor, withdrawalID int64) (*service.CustomReferralWithdrawal, error) {
	rows, err := exec.QueryContext(ctx, `
SELECT w.id,
       w.affiliate_id,
       a.user_id,
       COALESCE(u.email, ''),
       a.invite_code,
       w.amount::double precision,
       w.fee_amount::double precision,
       w.net_amount::double precision,
       w.account_type,
       COALESCE(w.account_name, ''),
       COALESCE(w.account_no, ''),
       COALESCE(w.account_network, ''),
       COALESCE(w.qr_image_url, ''),
       COALESCE(w.contact_info, ''),
       COALESCE(w.applicant_note, ''),
       COALESCE(w.admin_note, ''),
       COALESCE(w.payment_proof_url, ''),
       COALESCE(w.payment_txn_no, ''),
       w.status,
       w.submitted_at,
       w.approved_at,
       w.payout_deadline_at,
       w.paid_at,
       w.rejected_at,
       w.canceled_at,
       COALESCE(w.reject_reason, '')
FROM custom_commission_withdrawals w
JOIN custom_affiliates a ON a.id = w.affiliate_id
LEFT JOIN users u ON u.id = a.user_id
WHERE w.id = $1`, withdrawalID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, service.ErrCustomReferralWithdrawalNotFound
	}
	return scanCustomReferralWithdrawal(rows)
}

func (r *customReferralRepository) withTx(ctx context.Context, fn func(txCtx context.Context, exec sqlQueryExecutor) error) error {
	if tx := dbent.TxFromContext(ctx); tx != nil {
		exec := txAwareSQLExecutor(ctx, r.sql, r.client)
		if exec == nil {
			return fmt.Errorf("custom referral tx executor is not configured")
		}
		return fn(ctx, exec)
	}

	tx, err := r.client.Tx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	txCtx := dbent.NewTxContext(ctx, tx)
	exec := txAwareSQLExecutor(txCtx, r.sql, r.client)
	if exec == nil {
		return fmt.Errorf("custom referral tx executor is not configured")
	}
	if err := fn(txCtx, exec); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *customReferralRepository) generateUniqueInviteCode(ctx context.Context, exec sqlQueryExecutor) (string, error) {
	for i := 0; i < customInviteCodeMaxAttempts; i++ {
		code, err := generateCustomInviteCode()
		if err != nil {
			return "", err
		}
		rows, queryErr := exec.QueryContext(ctx, `SELECT 1 FROM custom_affiliates WHERE invite_code = $1`, code)
		if queryErr != nil {
			return "", queryErr
		}
		exists := rows.Next()
		if closeErr := rows.Close(); closeErr != nil {
			return "", closeErr
		}
		if !exists {
			return code, nil
		}
	}
	return "", fmt.Errorf("unable to generate unique custom invite code")
}

func generateCustomInviteCode() (string, error) {
	buf := make([]byte, customInviteCodeLength)
	_, err := rand.Read(buf)
	if err != nil {
		return "", err
	}
	for i := range buf {
		buf[i] = customInviteCodeCharset[int(buf[i])%len(customInviteCodeCharset)]
	}
	return string(buf), nil
}

func generateSettlementBatchNo() (string, error) {
	buf := make([]byte, 4)
	_, err := rand.Read(buf)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("settle-%s-%x", time.Now().UTC().Format("20060102150405"), buf), nil
}

func scanCustomAffiliate(scanner interface{ Scan(dest ...any) error }) (*service.CustomAffiliate, error) {
	var out service.CustomAffiliate
	var rate sql.NullFloat64
	var approvedAt sql.NullTime
	var disabledAt sql.NullTime
	if err := scanner.Scan(
		&out.ID,
		&out.UserID,
		&out.Email,
		&out.Username,
		&out.InviteCode,
		&out.Status,
		&out.SourceType,
		&rate,
		&out.AcquisitionEnabled,
		&out.SettlementEnabled,
		&out.WithdrawalEnabled,
		&out.RiskReason,
		&out.RiskNote,
		&approvedAt,
		&disabledAt,
	); err != nil {
		return nil, err
	}
	if rate.Valid {
		out.RateOverride = &rate.Float64
	}
	if approvedAt.Valid {
		out.ApprovedAt = &approvedAt.Time
	}
	if disabledAt.Valid {
		out.DisabledAt = &disabledAt.Time
	}
	return &out, nil
}

func scanCustomAffiliateWithStats(scanner interface{ Scan(dest ...any) error }) (*service.CustomAffiliate, error) {
	var out service.CustomAffiliate
	var rate sql.NullFloat64
	var approvedAt sql.NullTime
	var disabledAt sql.NullTime
	if err := scanner.Scan(
		&out.ID,
		&out.UserID,
		&out.Email,
		&out.Username,
		&out.InviteCode,
		&out.Status,
		&out.SourceType,
		&rate,
		&out.ClickCount,
		&out.BoundUserCount,
		&out.PaidUserCount,
		&out.PendingAmount,
		&out.AvailableAmount,
		&out.WithdrawnAmount,
		&out.AcquisitionEnabled,
		&out.SettlementEnabled,
		&out.WithdrawalEnabled,
		&out.RiskReason,
		&out.RiskNote,
		&approvedAt,
		&disabledAt,
	); err != nil {
		return nil, err
	}
	if rate.Valid {
		out.RateOverride = &rate.Float64
	}
	if approvedAt.Valid {
		out.ApprovedAt = &approvedAt.Time
	}
	if disabledAt.Valid {
		out.DisabledAt = &disabledAt.Time
	}
	return &out, nil
}

func scanCustomReferralWithdrawal(scanner interface{ Scan(dest ...any) error }) (*service.CustomReferralWithdrawal, error) {
	var out service.CustomReferralWithdrawal
	var approvedAt sql.NullTime
	var payoutDeadlineAt sql.NullTime
	var paidAt sql.NullTime
	var rejectedAt sql.NullTime
	var canceledAt sql.NullTime
	if err := scanner.Scan(
		&out.ID,
		&out.AffiliateID,
		&out.AffiliateUserID,
		&out.AffiliateEmail,
		&out.InviteCode,
		&out.Amount,
		&out.FeeAmount,
		&out.NetAmount,
		&out.AccountType,
		&out.AccountName,
		&out.AccountNo,
		&out.AccountNetwork,
		&out.QRImageURL,
		&out.ContactInfo,
		&out.ApplicantNote,
		&out.AdminNote,
		&out.PaymentProofURL,
		&out.PaymentTxnNo,
		&out.Status,
		&out.SubmittedAt,
		&approvedAt,
		&payoutDeadlineAt,
		&paidAt,
		&rejectedAt,
		&canceledAt,
		&out.RejectReason,
	); err != nil {
		return nil, err
	}
	if approvedAt.Valid {
		out.ApprovedAt = &approvedAt.Time
	}
	if payoutDeadlineAt.Valid {
		out.PayoutDeadlineAt = &payoutDeadlineAt.Time
	}
	if paidAt.Valid {
		out.PaidAt = &paidAt.Time
	}
	if rejectedAt.Valid {
		out.RejectedAt = &rejectedAt.Time
	}
	if canceledAt.Valid {
		out.CanceledAt = &canceledAt.Time
	}
	return &out, nil
}

func scanCustomReferralCommission(scanner interface{ Scan(dest ...any) error }) (*service.CustomReferralCommission, error) {
	var out service.CustomReferralCommission
	var availableAt sql.NullTime
	var reversedAt sql.NullTime
	if err := scanner.Scan(
		&out.ID,
		&out.AffiliateID,
		&out.AffiliateUserID,
		&out.AffiliateEmail,
		&out.OrderID,
		&out.InviteeUserID,
		&out.InviteeEmail,
		&out.InviteeUsername,
		&out.OrderType,
		&out.BaseAmount,
		&out.Rate,
		&out.CommissionAmount,
		&out.RefundedAmount,
		&out.Status,
		&out.SettleAt,
		&availableAt,
		&reversedAt,
		&out.ReversedReason,
		&out.CreatedAt,
	); err != nil {
		return nil, err
	}
	if availableAt.Valid {
		out.AvailableAt = &availableAt.Time
	}
	if reversedAt.Valid {
		out.ReversedAt = &reversedAt.Time
	}
	return &out, nil
}

func adminIDOrNil(v int64) any {
	if v <= 0 {
		return nil
	}
	return v
}

func customRoundTo(v float64, scale int) float64 {
	factor := math.Pow10(scale)
	return math.Round(v*factor) / factor
}

func isPQUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

func nearlyEqual(a, b float64) bool {
	return math.Abs(a-b) <= 0.00000001
}
