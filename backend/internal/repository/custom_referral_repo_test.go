package repository

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestComputeCustomCommissionManualReversalAllocation(t *testing.T) {
	dec := func(v float64) decimal.Decimal {
		return decimal.NewFromFloat(v).Round(8)
	}
	assertDec := func(t *testing.T, got decimal.Decimal, want float64) {
		t.Helper()
		require.Truef(t, got.Equal(dec(want)), "got %s, want %.8f", got.StringFixed(8), want)
	}

	tests := []struct {
		name          string
		input         customCommissionManualReversalAllocationInput
		wantPending   float64
		wantAvailable float64
		wantFrozen    float64
		wantDebt      float64
	}{
		{
			name: "pending deducts pending balance",
			input: customCommissionManualReversalAllocationInput{
				Status:         service.CustomReferralCommissionStatusPending,
				ReverseAmount:  dec(10),
				PendingBalance: dec(10),
			},
			wantPending: 10,
		},
		{
			name: "frozen deducts frozen withdrawal allocation",
			input: customCommissionManualReversalAllocationInput{
				Status:           service.CustomReferralCommissionStatusAvailable,
				ReverseAmount:    dec(10),
				CommissionAmount: dec(10),
				FrozenBalance:    dec(10),
				FrozenAllocated:  dec(10),
			},
			wantFrozen: 10,
		},
		{
			name: "available deducts unallocated available commission",
			input: customCommissionManualReversalAllocationInput{
				Status:           service.CustomReferralCommissionStatusAvailable,
				ReverseAmount:    dec(10),
				CommissionAmount: dec(10),
				AvailableBalance: dec(10),
			},
			wantAvailable: 10,
		},
		{
			name: "withdrawn becomes debt instead of negative balance",
			input: customCommissionManualReversalAllocationInput{
				Status:             service.CustomReferralCommissionStatusAvailable,
				ReverseAmount:      dec(10),
				CommissionAmount:   dec(10),
				WithdrawnAllocated: dec(10),
			},
			wantDebt: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeCustomCommissionManualReversalAllocation(tt.input)
			assertDec(t, got.PendingDecrease, tt.wantPending)
			assertDec(t, got.AvailableDecrease, tt.wantAvailable)
			assertDec(t, got.FrozenDecrease, tt.wantFrozen)
			assertDec(t, got.DebtIncrease, tt.wantDebt)
		})
	}
}

func TestRecordAdminAuditWithExecutorPersistsManualReverseContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repo := &customReferralRepository{}
	mock.ExpectExec("INSERT INTO custom_referral_admin_audit_logs").
		WithArgs(
			"commission_manual_reverse",
			int64(1001),
			int64(2002),
			int64(3003),
			"refund from payment console",
			"203.0.113.9",
			"browser-a",
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = repo.recordAdminAuditWithExecutor(context.Background(), db, 1001, 2002, service.CustomReferralAdminAuditContext{
		Action:      "commission_manual_reverse",
		AdminUserID: 3003,
		IP:          "203.0.113.9",
		UserAgent:   "browser-a",
		Reason:      "refund from payment console",
		OldValue: map[string]any{
			"commission_id":    int64(4004),
			"available_amount": 10.0,
		},
		NewValue: map[string]any{
			"commission_id":    int64(4004),
			"available_amount": 0.0,
			"idempotency_key":  "manual-reverse-1",
		},
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCustomReferralCommissionOrderFilter(t *testing.T) {
	tests := []struct {
		name    string
		columns map[string]struct{}
		alias   string
		want    string
	}{
		{
			name: "legacy order_id only",
			columns: map[string]struct{}{
				"order_id": {},
			},
			want: "order_id = $1",
		},
		{
			name: "source_order_id only",
			columns: map[string]struct{}{
				"source_order_id": {},
			},
			alias: "c",
			want:  "c.source_order_id = $1",
		},
		{
			name: "compat columns present",
			columns: map[string]struct{}{
				"order_id":        {},
				"source_order_id": {},
			},
			alias: "c",
			want:  "(c.order_id = $1 OR c.source_order_id = $1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := customReferralCommissionOrderFilter(tt.columns, tt.alias, "$1")
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCustomReferralCommissionOrderSelectExpr(t *testing.T) {
	tests := []struct {
		name    string
		columns map[string]struct{}
		alias   string
		want    string
	}{
		{
			name: "legacy order_id only",
			columns: map[string]struct{}{
				"order_id": {},
			},
			want: "order_id",
		},
		{
			name: "source_order_id only",
			columns: map[string]struct{}{
				"source_order_id": {},
			},
			alias: "c",
			want:  "c.source_order_id",
		},
		{
			name: "compat columns present",
			columns: map[string]struct{}{
				"order_id":        {},
				"source_order_id": {},
			},
			alias: "c",
			want:  "COALESCE(c.order_id, c.source_order_id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := customReferralCommissionOrderSelectExpr(tt.columns, tt.alias)
			require.Equal(t, tt.want, got)
		})
	}
}
