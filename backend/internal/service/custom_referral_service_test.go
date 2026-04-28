package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type customReferralSettingRepoStub struct {
	values map[string]string
}

func (s *customReferralSettingRepoStub) Get(context.Context, string) (*Setting, error) {
	return nil, nil
}

func (s *customReferralSettingRepoStub) GetValue(_ context.Context, key string) (string, error) {
	return s.values[key], nil
}

func (s *customReferralSettingRepoStub) Set(context.Context, string, string) error { return nil }

func (s *customReferralSettingRepoStub) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		out[key] = s.values[key]
	}
	return out, nil
}

func (s *customReferralSettingRepoStub) SetMultiple(context.Context, map[string]string) error {
	return nil
}

func (s *customReferralSettingRepoStub) GetAll(context.Context) (map[string]string, error) {
	return s.values, nil
}

func (s *customReferralSettingRepoStub) Delete(context.Context, string) error { return nil }

type customReferralReverseRepoStub struct {
	CustomReferralRepository
	called       bool
	input        CustomReferralRefundInput
	manualCalled bool
	manualInput  CustomReferralManualReverseInput
}

func (s *customReferralReverseRepoStub) ReverseCommissionForRefund(_ context.Context, input CustomReferralRefundInput) (float64, error) {
	s.called = true
	s.input = input
	return 12.5, nil
}

func (s *customReferralReverseRepoStub) ReverseCommissionManually(_ context.Context, input CustomReferralManualReverseInput) (*CustomReferralCommissionReversal, error) {
	s.manualCalled = true
	s.manualInput = input
	return &CustomReferralCommissionReversal{
		ID:            1,
		CommissionID:  2,
		OrderID:       input.OrderID,
		RefundAmount:  input.RefundAmount,
		ReverseAmount: 3,
		ExternalRefID: input.IdempotencyKey,
		AdminUserID:   input.AdminUserID,
		CreatedAt:     input.ReversedAt,
	}, nil
}

type customReferralP2RepoStub struct {
	CustomReferralRepository
	commissionInput CustomReferralOrderInput
	withdrawCalled  bool
}

func (s *customReferralP2RepoStub) CreatePendingCommissionForOrder(_ context.Context, input CustomReferralOrderInput, freezeDays int) (float64, error) {
	s.commissionInput = input
	return 12.34 + float64(freezeDays*0), nil
}

func (s *customReferralP2RepoStub) SettleDueCommissions(context.Context, time.Time) error {
	return nil
}

func (s *customReferralP2RepoStub) CreateWithdrawal(context.Context, CustomReferralWithdrawalCreateInput, float64) (*CustomReferralWithdrawal, error) {
	s.withdrawCalled = true
	return &CustomReferralWithdrawal{ID: 1}, nil
}

type customReferralP3RepoStub struct {
	CustomReferralRepository
	affiliate *CustomAffiliate
	err       error
}

func (s *customReferralP3RepoStub) GetApprovedAffiliateByCode(context.Context, string) (*CustomAffiliate, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.affiliate, nil
}

type customReferralCycleRepoStub struct {
	CustomReferralRepository
	affiliateByCode map[string]*CustomAffiliate
	ancestorsByUser map[int64][]int64
	bindCalled      bool
	lastBindSource  string
	lastBindCode    string
}

func (r *customReferralCycleRepoStub) GetApprovedAffiliateByCode(_ context.Context, code string) (*CustomAffiliate, error) {
	return r.affiliateByCode[strings.ToUpper(strings.TrimSpace(code))], nil
}

func (r *customReferralCycleRepoStub) InviteeInInviterAncestorChain(_ context.Context, inviteeUserID, inviterUserID int64, maxDepth int) (bool, error) {
	if inviteeUserID == inviterUserID {
		return true, nil
	}
	if maxDepth != customReferralMaxAncestorDepth {
		return false, ErrCustomReferralChainTooDeep
	}
	for _, ancestorID := range r.ancestorsByUser[inviterUserID] {
		if ancestorID == inviteeUserID {
			return true, nil
		}
	}
	return false, nil
}

func (r *customReferralCycleRepoStub) BindInvitee(_ context.Context, _ int64, _ int64, _ int64, bindSource string, bindCode string, _ time.Time) (bool, error) {
	r.bindCalled = true
	r.lastBindSource = bindSource
	r.lastBindCode = bindCode
	return true, nil
}

type customReferralCycleSettingRepo map[string]string

func (r customReferralCycleSettingRepo) Get(_ context.Context, key string) (*Setting, error) {
	if v, ok := r[key]; ok {
		return &Setting{Key: key, Value: v}, nil
	}
	return nil, sql.ErrNoRows
}

func (r customReferralCycleSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	if v, ok := r[key]; ok {
		return v, nil
	}
	return "", sql.ErrNoRows
}

func (r customReferralCycleSettingRepo) Set(_ context.Context, key, value string) error {
	r[key] = value
	return nil
}

func (r customReferralCycleSettingRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		if v, ok := r[key]; ok {
			out[key] = v
		}
	}
	return out, nil
}

func (r customReferralCycleSettingRepo) SetMultiple(_ context.Context, settings map[string]string) error {
	for key, value := range settings {
		r[key] = value
	}
	return nil
}

func (r customReferralCycleSettingRepo) GetAll(context.Context) (map[string]string, error) {
	out := make(map[string]string, len(r))
	for key, value := range r {
		out[key] = value
	}
	return out, nil
}

func (r customReferralCycleSettingRepo) Delete(_ context.Context, key string) error {
	delete(r, key)
	return nil
}

func TestPublicSettingsDerivesAffiliateEnabledFromCustomReferralProvider(t *testing.T) {
	ctx := context.Background()
	repo := &customReferralSettingRepoStub{values: map[string]string{
		SettingKeyCustomReferralProvider: CustomReferralProviderCustom,
	}}
	svc := NewSettingService(repo, &config.Config{})

	settings, err := svc.GetPublicSettings(ctx)
	if err != nil {
		t.Fatalf("GetPublicSettings returned error: %v", err)
	}
	if !settings.AffiliateEnabled {
		t.Fatalf("AffiliateEnabled = false, want true")
	}

	repo.values[SettingKeyCustomReferralProvider] = CustomReferralProviderDisabled
	settings, err = svc.GetPublicSettings(ctx)
	if err != nil {
		t.Fatalf("GetPublicSettings returned error: %v", err)
	}
	if settings.AffiliateEnabled {
		t.Fatalf("AffiliateEnabled = true, want false")
	}
}

func TestCustomReferralDisabledRejectsApplicationAndWithdrawal(t *testing.T) {
	ctx := context.Background()
	svc := NewCustomReferralService(nil, &customReferralSettingRepoStub{values: map[string]string{
		SettingKeyCustomReferralProvider: CustomReferralProviderDisabled,
	}}, nil)

	if _, err := svc.ApplyAffiliate(ctx, 1, ""); !errors.Is(err, ErrCustomReferralAffiliateDisabled) {
		t.Fatalf("ApplyAffiliate error = %v, want %v", err, ErrCustomReferralAffiliateDisabled)
	}
	if _, err := svc.CreateWithdrawal(ctx, CustomReferralWithdrawalCreateInput{UserID: 1, Amount: 100}); !errors.Is(err, ErrCustomReferralWithdrawDisabled) {
		t.Fatalf("CreateWithdrawal error = %v, want %v", err, ErrCustomReferralWithdrawDisabled)
	}
}

func TestReverseCommissionForRefundIgnoresProviderSwitch(t *testing.T) {
	ctx := context.Background()
	repo := &customReferralReverseRepoStub{}
	svc := NewCustomReferralService(repo, &customReferralSettingRepoStub{values: map[string]string{
		SettingKeyCustomReferralProvider: CustomReferralProviderDisabled,
	}}, nil)

	refundedAt := time.Now()
	reversed, err := svc.ReverseCommissionForRefund(ctx, CustomReferralRefundInput{
		OrderID:      1001,
		RefundAmount: 50,
		Reason:       "refund",
		RefundedAt:   refundedAt,
	})
	if err != nil {
		t.Fatalf("ReverseCommissionForRefund returned error: %v", err)
	}
	if reversed != 12.5 {
		t.Fatalf("reversed = %v, want 12.5", reversed)
	}
	if !repo.called {
		t.Fatalf("ReverseCommissionForRefund did not reach repository when provider was disabled")
	}
	if repo.input.OrderID != 1001 || repo.input.RefundAmount != 50 || !repo.input.RefundedAt.Equal(refundedAt) {
		t.Fatalf("repository input = %+v, want original refund input", repo.input)
	}
}

func TestCreateCommissionForOrderUsesOrderSnapshotRate(t *testing.T) {
	ctx := context.Background()
	repo := &customReferralP2RepoStub{}
	svc := NewCustomReferralService(repo, &customReferralSettingRepoStub{values: map[string]string{
		SettingKeyCustomReferralProvider:         CustomReferralProviderCustom,
		SettingKeyCustomReferralDefaultRate:      "5",
		SettingKeyCustomReferralSettleFreezeDays: "7",
	}}, nil)

	amount, err := svc.CreateCommissionForOrder(ctx, CustomReferralOrderInput{
		OrderID:     101,
		UserID:      202,
		AffiliateID: 303,
		BaseAmount:  99.99,
		Rate:        12.5,
		OrderType:   "balance",
	})
	if err != nil {
		t.Fatalf("CreateCommissionForOrder returned error: %v", err)
	}
	if amount != 12.34 {
		t.Fatalf("amount = %v, want 12.34", amount)
	}
	if repo.commissionInput.AffiliateID != 303 || repo.commissionInput.Rate != 12.5 || repo.commissionInput.BaseAmount != 99.99 {
		t.Fatalf("repository input did not preserve order snapshot: %+v", repo.commissionInput)
	}
}

func TestCreateWithdrawalRequiresIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	repo := &customReferralP2RepoStub{}
	svc := NewCustomReferralService(repo, &customReferralSettingRepoStub{values: map[string]string{
		SettingKeyCustomReferralProvider:          CustomReferralProviderCustom,
		SettingKeyCustomReferralDefaultRate:       "5",
		SettingKeyCustomReferralMinWithdrawAmount: "1",
	}}, nil)

	_, err := svc.CreateWithdrawal(ctx, CustomReferralWithdrawalCreateInput{
		UserID:      1,
		Amount:      10,
		AccountType: "alipay",
		AccountNo:   "acct",
	})
	if !errors.Is(err, ErrCustomReferralInvalidIdempotency) {
		t.Fatalf("CreateWithdrawal error = %v, want %v", err, ErrCustomReferralInvalidIdempotency)
	}
	if repo.withdrawCalled {
		t.Fatalf("CreateWithdrawal reached repository without idempotency key")
	}
}

func TestValidateInviteCodeForSignupRejectsInvalidAndDisabledCodes(t *testing.T) {
	ctx := context.Background()
	settings := &customReferralSettingRepoStub{values: map[string]string{
		SettingKeyCustomReferralProvider: CustomReferralProviderCustom,
	}}

	missing := NewCustomReferralService(&customReferralP3RepoStub{}, settings, nil)
	if err := missing.ValidateInviteCodeForSignup(ctx, "missing"); !errors.Is(err, ErrCustomReferralAffiliateDisabled) {
		t.Fatalf("missing affiliate error = %v, want disabled/not found style rejection", err)
	}

	disabled := NewCustomReferralService(&customReferralP3RepoStub{affiliate: &CustomAffiliate{
		ID:                 1,
		UserID:             2,
		InviteCode:         "INVITE",
		AcquisitionEnabled: false,
	}}, settings, nil)
	if err := disabled.ValidateInviteCodeForSignup(ctx, "INVITE"); !errors.Is(err, ErrCustomReferralAffiliateDisabled) {
		t.Fatalf("disabled affiliate error = %v, want %v", err, ErrCustomReferralAffiliateDisabled)
	}
}

func TestValidateInviteCodeForSignupAcceptsApprovedAcquisitionCode(t *testing.T) {
	ctx := context.Background()
	svc := NewCustomReferralService(&customReferralP3RepoStub{affiliate: &CustomAffiliate{
		ID:                 1,
		UserID:             2,
		InviteCode:         "INVITE",
		AcquisitionEnabled: true,
	}}, &customReferralSettingRepoStub{values: map[string]string{
		SettingKeyCustomReferralProvider: CustomReferralProviderCustom,
	}}, nil)

	if err := svc.ValidateInviteCodeForSignup(ctx, " invite "); err != nil {
		t.Fatalf("ValidateInviteCodeForSignup returned error: %v", err)
	}
}

func TestCustomReferralBindInviteeByCodeRejectsCycles(t *testing.T) {
	tests := []struct {
		name          string
		inviteeUserID int64
		affiliate     *CustomAffiliate
		ancestors     map[int64][]int64
		wantErr       error
		wantBound     bool
	}{
		{
			name:          "direct self invite rejected",
			inviteeUserID: 1,
			affiliate:     &CustomAffiliate{ID: 10, UserID: 1, InviteCode: "AAA", Status: CustomAffiliateStatusApproved, AcquisitionEnabled: true},
			wantErr:       ErrCustomReferralSelfInvite,
		},
		{
			name:          "A invites B then B invites A rejected",
			inviteeUserID: 1,
			affiliate:     &CustomAffiliate{ID: 20, UserID: 2, InviteCode: "BBB", Status: CustomAffiliateStatusApproved, AcquisitionEnabled: true},
			ancestors:     map[int64][]int64{2: {1}},
			wantErr:       ErrCustomReferralCycleInvite,
		},
		{
			name:          "A to B to C then C invites A rejected",
			inviteeUserID: 1,
			affiliate:     &CustomAffiliate{ID: 30, UserID: 3, InviteCode: "CCC", Status: CustomAffiliateStatusApproved, AcquisitionEnabled: true},
			ancestors:     map[int64][]int64{3: {2, 1}},
			wantErr:       ErrCustomReferralCycleInvite,
		},
		{
			name:          "normal A to B to C allowed",
			inviteeUserID: 3,
			affiliate:     &CustomAffiliate{ID: 20, UserID: 2, InviteCode: "BBB", Status: CustomAffiliateStatusApproved, AcquisitionEnabled: true},
			ancestors:     map[int64][]int64{2: {1}},
			wantBound:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &customReferralCycleRepoStub{
				affiliateByCode: map[string]*CustomAffiliate{tt.affiliate.InviteCode: tt.affiliate},
				ancestorsByUser: tt.ancestors,
			}
			settings := customReferralCycleSettingRepo{
				SettingKeyCustomReferralProvider: CustomReferralProviderCustom,
			}
			svc := NewCustomReferralService(repo, settings, nil)
			err := svc.BindInviteeByCode(context.Background(), tt.inviteeUserID, tt.affiliate.InviteCode)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("BindInviteeByCode error = %v, want %v", err, tt.wantErr)
			}
			if repo.bindCalled != tt.wantBound {
				t.Fatalf("BindInvitee called = %v, want %v", repo.bindCalled, tt.wantBound)
			}
		})
	}
}

func TestUserCommissionDTOOmitsSensitiveFields(t *testing.T) {
	payload, err := json.Marshal(CustomReferralUserCommission{
		ID:               10,
		OrderType:        "balance",
		CommissionAmount: 1.23,
		RefundedAmount:   0.45,
		Status:           CustomReferralCommissionStatusAvailable,
		SettleAt:         time.Unix(100, 0),
		CreatedAt:        time.Unix(90, 0),
	})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	body := string(payload)
	for _, forbidden := range []string{
		"invitee_email",
		"order_id",
		"pay_amount",
		"base_amount",
		"commission_base_amount",
		"rate",
		"reversed_reason",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("user commission payload leaked %q: %s", forbidden, body)
		}
	}
}

func TestCustomReferralBindInviteeByCodeUsesContextSource(t *testing.T) {
	repo := &customReferralCycleRepoStub{
		affiliateByCode: map[string]*CustomAffiliate{
			"ABC123": {ID: 10, UserID: 20, InviteCode: "ABC123", Status: CustomAffiliateStatusApproved, AcquisitionEnabled: true},
		},
	}
	settings := customReferralCycleSettingRepo{
		SettingKeyCustomReferralProvider: CustomReferralProviderCustom,
	}
	svc := NewCustomReferralService(repo, settings, nil)

	ctx := ContextWithAffiliateAttribution(context.Background(), "ABC123", AffiliateBindingSourceCode)
	if err := svc.BindInviteeByCode(ctx, 30, "ABC123"); err != nil {
		t.Fatalf("BindInviteeByCode error = %v", err)
	}
	if repo.lastBindSource != AffiliateBindingSourceCode {
		t.Fatalf("BindInvitee source = %q, want %q", repo.lastBindSource, AffiliateBindingSourceCode)
	}
	if repo.lastBindCode != "ABC123" {
		t.Fatalf("BindInvitee code = %q, want %q", repo.lastBindCode, "ABC123")
	}
}

func TestAdminCommissionDTOKeepsAuditFields(t *testing.T) {
	payload, err := json.Marshal(CustomReferralCommission{
		ID:               10,
		AffiliateID:      20,
		OrderID:          30,
		InviteeEmail:     "invitee@example.com",
		OrderType:        "balance",
		BaseAmount:       100,
		Rate:             10,
		CommissionAmount: 10,
		Status:           CustomReferralCommissionStatusPending,
		SettleAt:         time.Unix(100, 0),
		ReversedReason:   "manual refund",
		CreatedAt:        time.Unix(90, 0),
	})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	body := string(payload)
	for _, required := range []string{
		"invitee_email",
		"order_id",
		"base_amount",
		"rate",
		"reversed_reason",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("admin commission payload missing %q: %s", required, body)
		}
	}
}

func TestReverseCommissionManuallyValidatesIdempotencyAndTarget(t *testing.T) {
	ctx := context.Background()
	repo := &customReferralReverseRepoStub{}
	svc := NewCustomReferralService(repo, nil, nil)

	_, err := svc.ReverseCommissionManually(ctx, CustomReferralManualReverseInput{
		OrderID:      1,
		RefundAmount: 10,
	})
	if !errors.Is(err, ErrCustomReferralInvalidIdempotency) {
		t.Fatalf("missing idempotency error = %v, want %v", err, ErrCustomReferralInvalidIdempotency)
	}

	_, err = svc.ReverseCommissionManually(ctx, CustomReferralManualReverseInput{
		RefundAmount:   10,
		IdempotencyKey: "refund-1",
	})
	if !errors.Is(err, ErrCustomReferralInvalidReverseInput) {
		t.Fatalf("missing target error = %v, want %v", err, ErrCustomReferralInvalidReverseInput)
	}

	_, err = svc.ReverseCommissionManually(ctx, CustomReferralManualReverseInput{
		OrderID:        1,
		RefundAmount:   10,
		IdempotencyKey: "refund-1",
	})
	if !errors.Is(err, ErrCustomReferralReverseReasonRequired) {
		t.Fatalf("missing reason error = %v, want %v", err, ErrCustomReferralReverseReasonRequired)
	}
}

func TestReverseCommissionManuallyNormalizesInputAndCallsRepository(t *testing.T) {
	ctx := context.Background()
	repo := &customReferralReverseRepoStub{}
	svc := NewCustomReferralService(repo, nil, nil)

	out, err := svc.ReverseCommissionManually(ctx, CustomReferralManualReverseInput{
		OrderID:        1001,
		RefundAmount:   66,
		Reason:         "  refund in payment console  ",
		IdempotencyKey: "  refund:1001  ",
		AdminUserID:    7,
		IP:             "203.0.113.9",
		UserAgent:      "browser-a",
	})
	if err != nil {
		t.Fatalf("ReverseCommissionManually returned error: %v", err)
	}
	if !repo.manualCalled {
		t.Fatalf("repository was not called")
	}
	if repo.manualInput.Reason != "refund in payment console" || repo.manualInput.IdempotencyKey != "refund:1001" {
		t.Fatalf("repository input was not normalized: %+v", repo.manualInput)
	}
	if repo.manualInput.AdminUserID != 7 || repo.manualInput.IP != "203.0.113.9" || repo.manualInput.UserAgent != "browser-a" {
		t.Fatalf("repository audit input was not preserved: %+v", repo.manualInput)
	}
	if repo.manualInput.ReversedAt.IsZero() || out.CreatedAt.IsZero() {
		t.Fatalf("expected ReversedAt to be filled")
	}
}
