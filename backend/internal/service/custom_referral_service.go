package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/moneyx"
	"github.com/shopspring/decimal"
)

const (
	CustomReferralProviderDisabled = "disabled"
	CustomReferralProviderCustom   = "custom"

	CustomAffiliateStatusPending  = "pending"
	CustomAffiliateStatusApproved = "approved"
	CustomAffiliateStatusRejected = "rejected"
	CustomAffiliateStatusDisabled = "disabled"

	CustomAffiliateSourceAdminCreated = "admin_created"
	CustomAffiliateSourceUserApplied  = "user_applied"

	CustomReferralCommissionStatusPending   = "pending"
	CustomReferralCommissionStatusAvailable = "available"
	CustomReferralCommissionStatusReversed  = "reversed"

	CustomReferralWithdrawalStatusPending  = "pending"
	CustomReferralWithdrawalStatusApproved = "approved"
	CustomReferralWithdrawalStatusPaid     = "paid"
	CustomReferralWithdrawalStatusRejected = "rejected"
	CustomReferralWithdrawalStatusCanceled = "canceled"

	CustomReferralWithdrawalItemStatusFrozen    = "frozen"
	CustomReferralWithdrawalItemStatusReleased  = "released"
	CustomReferralWithdrawalItemStatusWithdrawn = "withdrawn"

	CustomReferralCommissionJobStatusPending    = "pending"
	CustomReferralCommissionJobStatusProcessing = "processing"
	CustomReferralCommissionJobStatusSucceeded  = "succeeded"
	CustomReferralCommissionJobStatusFailed     = "failed"

	CustomReferralCookieName = "custom_referral"

	customReferralDefaultCookieTTLDays = 30
	customReferralDefaultFreezeDays    = 15
	customReferralDefaultRedirectPath  = "/register"
	customReferralMaxAncestorDepth     = 64
)

var (
	ErrCustomReferralAffiliateNotFound     = infraerrors.NotFound("CUSTOM_REFERRAL_AFFILIATE_NOT_FOUND", "affiliate not found")
	ErrCustomReferralAffiliateDisabled     = infraerrors.Forbidden("CUSTOM_REFERRAL_AFFILIATE_DISABLED", "affiliate is disabled")
	ErrCustomReferralSelfInvite            = infraerrors.BadRequest("CUSTOM_REFERRAL_SELF_INVITE", "self referral is not allowed")
	ErrCustomReferralCycleInvite           = infraerrors.BadRequest("CUSTOM_REFERRAL_CYCLE_INVITE", "cyclic referral is not allowed")
	ErrCustomReferralChainTooDeep          = infraerrors.BadRequest("CUSTOM_REFERRAL_CHAIN_TOO_DEEP", "referral chain is too deep")
	ErrCustomReferralAlreadyBound          = infraerrors.Conflict("CUSTOM_REFERRAL_ALREADY_BOUND", "referral binding already exists")
	ErrCustomReferralRateNotConfigured     = infraerrors.BadRequest("CUSTOM_REFERRAL_RATE_NOT_CONFIGURED", "default referral rate is not configured")
	ErrCustomReferralPermissionDenied      = infraerrors.Forbidden("CUSTOM_REFERRAL_PERMISSION_DENIED", "user is not an approved affiliate")
	ErrCustomReferralAlreadyApproved       = infraerrors.Conflict("CUSTOM_REFERRAL_ALREADY_APPROVED", "user is already an approved affiliate")
	ErrCustomReferralAdjustInsufficient    = infraerrors.BadRequest("CUSTOM_REFERRAL_ADJUST_INSUFFICIENT", "insufficient available commission for adjustment")
	ErrCustomReferralInvalidWithdrawType   = infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_WITHDRAW_TYPE", "invalid withdrawal account type")
	ErrCustomReferralInvalidWithdrawNet    = infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_WITHDRAW_NETWORK", "invalid withdrawal network")
	ErrCustomReferralWithdrawAccountEmpty  = infraerrors.BadRequest("CUSTOM_REFERRAL_WITHDRAW_ACCOUNT_REQUIRED", "withdrawal account is required")
	ErrCustomReferralWithdrawDisabled      = infraerrors.Forbidden("CUSTOM_REFERRAL_WITHDRAW_DISABLED", "withdrawal is disabled")
	ErrCustomReferralWithdrawTooSmall      = infraerrors.BadRequest("CUSTOM_REFERRAL_WITHDRAW_TOO_SMALL", "withdrawal amount is below minimum")
	ErrCustomReferralWithdrawInsufficient  = infraerrors.BadRequest("CUSTOM_REFERRAL_WITHDRAW_INSUFFICIENT", "insufficient available commission")
	ErrCustomReferralWithdrawalNotFound    = infraerrors.NotFound("CUSTOM_REFERRAL_WITHDRAWAL_NOT_FOUND", "withdrawal not found")
	ErrCustomReferralInvalidIdempotency    = infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_IDEMPOTENCY_KEY", "invalid idempotency key")
	ErrCustomReferralCommissionNotFound    = infraerrors.NotFound("CUSTOM_REFERRAL_COMMISSION_NOT_FOUND", "commission not found")
	ErrCustomReferralInvalidReverseInput   = infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_REVERSE_INPUT", "invalid commission reversal input")
	ErrCustomReferralReverseReasonRequired = infraerrors.BadRequest("CUSTOM_REFERRAL_REVERSE_REASON_REQUIRED", "commission reversal reason is required")
	ErrCustomReferralInvalidAsset          = infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_ASSET", "invalid referral asset url")
)

type CustomReferralConfig struct {
	Provider          string
	CookieTTLDays     int
	DefaultRate       float64
	HasDefaultRate    bool
	SettleFreezeDays  int
	MinWithdrawAmount float64
	WithdrawFee       float64
}

type CustomReferralAdminConfig struct {
	Provider          string  `json:"provider"`
	CookieTTLDays     int     `json:"cookie_ttl_days"`
	DefaultRate       float64 `json:"default_rate"`
	HasDefaultRate    bool    `json:"has_default_rate"`
	SettleFreezeDays  int     `json:"settle_freeze_days"`
	MinWithdrawAmount float64 `json:"min_withdraw_amount"`
	WithdrawFee       float64 `json:"withdraw_fee"`
}

type CustomAffiliate struct {
	ID                 int64      `json:"id"`
	UserID             int64      `json:"user_id"`
	Email              string     `json:"email,omitempty"`
	Username           string     `json:"username,omitempty"`
	InviteCode         string     `json:"invite_code"`
	Status             string     `json:"status"`
	SourceType         string     `json:"source_type"`
	RateOverride       *float64   `json:"rate_override,omitempty"`
	ClickCount         int64      `json:"click_count"`
	BoundUserCount     int64      `json:"bound_user_count"`
	PaidUserCount      int64      `json:"paid_user_count"`
	PendingAmount      float64    `json:"pending_amount"`
	AvailableAmount    float64    `json:"available_amount"`
	WithdrawnAmount    float64    `json:"withdrawn_amount"`
	AcquisitionEnabled bool       `json:"acquisition_enabled"`
	SettlementEnabled  bool       `json:"settlement_enabled"`
	WithdrawalEnabled  bool       `json:"withdrawal_enabled"`
	RiskReason         string     `json:"risk_reason,omitempty"`
	RiskNote           string     `json:"risk_note,omitempty"`
	ApprovedAt         *time.Time `json:"approved_at,omitempty"`
	DisabledAt         *time.Time `json:"disabled_at,omitempty"`
}

type CustomReferralDashboard struct {
	Status             string   `json:"status"`
	InviteCode         string   `json:"invite_code"`
	Rate               *float64 `json:"rate,omitempty"`
	MinWithdrawAmount  float64  `json:"min_withdraw_amount"`
	ClickCount         int64    `json:"click_count"`
	BoundUserCount     int64    `json:"bound_user_count"`
	PaidUserCount      int64    `json:"paid_user_count"`
	PendingAmount      float64  `json:"pending_amount"`
	AvailableAmount    float64  `json:"available_amount"`
	FrozenAmount       float64  `json:"frozen_amount"`
	WithdrawnAmount    float64  `json:"withdrawn_amount"`
	ReversedAmount     float64  `json:"reversed_amount"`
	DebtAmount         float64  `json:"debt_amount"`
	AcquisitionEnabled bool     `json:"acquisition_enabled"`
	SettlementEnabled  bool     `json:"settlement_enabled"`
	WithdrawalEnabled  bool     `json:"withdrawal_enabled"`
}

type CustomReferralAdminOverview struct {
	TotalAffiliates        int64   `json:"total_affiliates"`
	ApprovedAffiliates     int64   `json:"approved_affiliates"`
	DisabledAffiliates     int64   `json:"disabled_affiliates"`
	PendingAmount          float64 `json:"pending_amount"`
	AvailableAmount        float64 `json:"available_amount"`
	FrozenAmount           float64 `json:"frozen_amount"`
	WithdrawnAmount        float64 `json:"withdrawn_amount"`
	ReferralClickCount     int64   `json:"referral_click_count"`
	BoundUserCount         int64   `json:"bound_user_count"`
	EffectivePaidUserCount int64   `json:"effective_paid_user_count"`
}

type CustomReferralLanding struct {
	Code          string
	RedirectPath  string
	CookieTTLDays int
}

type CustomReferralClickInput struct {
	Referer       string
	LandingPath   string
	IPHash        string
	UserAgentHash string
	ClickedAt     time.Time
}

type CustomReferralListParams struct {
	Page     int
	PageSize int
	Status   string
	Keyword  string
}

type CustomReferralWithdrawalListParams struct {
	Page     int
	PageSize int
	Status   string
}

type CustomReferralCommissionListParams struct {
	Page     int
	PageSize int
	Status   string
}

type CustomReferralOrderInput struct {
	OrderID           int64
	UserID            int64
	AffiliateID       int64
	OrderType         string
	BaseAmount        float64
	BaseAmountDecimal decimal.Decimal
	Rate              float64
	RateDecimal       decimal.Decimal
	PaidAt            time.Time
}

type CustomReferralOrderSnapshot struct {
	AffiliateID int64
	Rate        float64
	RateDecimal decimal.Decimal
}

type CustomReferralAdminAuditContext struct {
	Action      string
	AdminUserID int64
	IP          string
	UserAgent   string
	Reason      string
	OldValue    map[string]any
	NewValue    map[string]any
}

type CustomReferralRefundInput struct {
	// Amount fields are kept as float64 for legacy API/Ent compatibility only.
	// Repository code converts them to moneyx/decimal before core money math.
	OrderID      int64
	RefundAmount float64
	Reason       string
	RefundedAt   time.Time
}

type CustomReferralManualReverseInput struct {
	// Amount fields are kept as float64 for legacy API/Ent compatibility only.
	// Repository code converts them to moneyx/decimal before core money math.
	OrderID        int64
	CommissionID   int64
	RefundAmount   float64
	Reason         string
	IdempotencyKey string
	AdminUserID    int64
	IP             string
	UserAgent      string
	ReversedAt     time.Time
}

type CustomReferralWithdrawalCreateInput struct {
	// Amount fields are kept as float64 for legacy API/Ent compatibility only.
	// Repository code converts them to moneyx/decimal before core money math.
	UserID         int64
	Amount         float64
	AccountType    string
	AccountName    string
	AccountNo      string
	AccountNetwork string
	QRImageURL     string
	ContactInfo    string
	ApplicantNote  string
	IdempotencyKey string
}

type CustomReferralAdjustInput struct {
	// Delta is kept for legacy API compatibility; DeltaDecimal is the normalized
	// value used by the repository for core balance math.
	UserID         int64
	AdminUserID    int64
	Delta          float64
	DeltaDecimal   decimal.Decimal
	Remark         string
	IdempotencyKey string
	Audit          CustomReferralAdminAuditContext
}

type CustomReferralWithdrawalReviewInput struct {
	WithdrawalID int64
	AdminUserID  int64
	AdminNote    string
	RejectReason string
}

type CustomReferralWithdrawalPayInput struct {
	WithdrawalID    int64
	AdminUserID     int64
	AdminNote       string
	PaymentProofURL string
	PaymentTxnNo    string
}

type CustomReferralWithdrawal struct {
	ID               int64      `json:"id"`
	AffiliateID      int64      `json:"affiliate_id"`
	AffiliateUserID  int64      `json:"affiliate_user_id"`
	AffiliateEmail   string     `json:"affiliate_email,omitempty"`
	InviteCode       string     `json:"invite_code,omitempty"`
	Amount           float64    `json:"amount"`
	FeeAmount        float64    `json:"fee_amount"`
	NetAmount        float64    `json:"net_amount"`
	AccountType      string     `json:"account_type"`
	AccountName      string     `json:"account_name"`
	AccountNo        string     `json:"account_no"`
	AccountNetwork   string     `json:"account_network"`
	QRImageURL       string     `json:"qr_image_url"`
	ContactInfo      string     `json:"contact_info"`
	ApplicantNote    string     `json:"applicant_note"`
	AdminNote        string     `json:"admin_note"`
	PaymentProofURL  string     `json:"payment_proof_url"`
	PaymentTxnNo     string     `json:"payment_txn_no"`
	Status           string     `json:"status"`
	SubmittedAt      time.Time  `json:"submitted_at"`
	ApprovedAt       *time.Time `json:"approved_at,omitempty"`
	PayoutDeadlineAt *time.Time `json:"payout_deadline_at,omitempty"`
	PaidAt           *time.Time `json:"paid_at,omitempty"`
	RejectedAt       *time.Time `json:"rejected_at,omitempty"`
	CanceledAt       *time.Time `json:"canceled_at,omitempty"`
	RejectReason     string     `json:"reject_reason"`
}

type CustomReferralCommission struct {
	ID               int64      `json:"id"`
	AffiliateID      int64      `json:"affiliate_id"`
	AffiliateUserID  int64      `json:"affiliate_user_id"`
	AffiliateEmail   string     `json:"affiliate_email,omitempty"`
	OrderID          int64      `json:"order_id"`
	InviteeUserID    int64      `json:"invitee_user_id"`
	InviteeEmail     string     `json:"invitee_email,omitempty"`
	InviteeUsername  string     `json:"invitee_username,omitempty"`
	OrderType        string     `json:"order_type"`
	BaseAmount       float64    `json:"base_amount"`
	Rate             float64    `json:"rate"`
	CommissionAmount float64    `json:"commission_amount"`
	RefundedAmount   float64    `json:"refunded_amount"`
	Status           string     `json:"status"`
	SettleAt         time.Time  `json:"settle_at"`
	AvailableAt      *time.Time `json:"available_at,omitempty"`
	ReversedAt       *time.Time `json:"reversed_at,omitempty"`
	ReversedReason   string     `json:"reversed_reason"`
	CreatedAt        time.Time  `json:"created_at"`
}

type CustomReferralUserCommission struct {
	ID               int64      `json:"id"`
	OrderType        string     `json:"order_type"`
	CommissionAmount float64    `json:"commission_amount"`
	RefundedAmount   float64    `json:"refunded_amount"`
	Status           string     `json:"status"`
	SettleAt         time.Time  `json:"settle_at"`
	AvailableAt      *time.Time `json:"available_at,omitempty"`
	ReversedAt       *time.Time `json:"reversed_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

type CustomReferralCommissionReversal struct {
	ID               int64     `json:"id"`
	AffiliateID      int64     `json:"affiliate_id"`
	CommissionID     int64     `json:"commission_id"`
	OrderID          int64     `json:"order_id"`
	RefundAmount     float64   `json:"refund_amount"`
	ReverseAmount    float64   `json:"reverse_amount"`
	DeltaPending     float64   `json:"delta_pending"`
	DeltaAvailable   float64   `json:"delta_available"`
	DeltaFrozen      float64   `json:"delta_frozen"`
	DeltaReversed    float64   `json:"delta_reversed"`
	DeltaDebt        float64   `json:"delta_debt"`
	Reason           string    `json:"reason"`
	ExternalRefID    string    `json:"external_ref_id"`
	AdminUserID      int64     `json:"admin_user_id"`
	CreatedAt        time.Time `json:"created_at"`
	AlreadyProcessed bool      `json:"already_processed,omitempty"`
}

type CustomReferralSettlementBatch struct {
	ID           int64      `json:"id"`
	BatchNo      string     `json:"batch_no"`
	Status       string     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	ScannedCount int        `json:"scanned_count"`
	SettledCount int        `json:"settled_count"`
	SkippedCount int        `json:"skipped_count"`
	FailedCount  int        `json:"failed_count"`
	ErrorSummary string     `json:"error_summary"`
}

type CustomReferralCommissionJob struct {
	ID           int64      `json:"id"`
	OrderID      int64      `json:"order_id"`
	AffiliateID  int64      `json:"affiliate_id,omitempty"`
	Status       string     `json:"status"`
	AttemptCount int        `json:"attempt_count"`
	LastError    string     `json:"last_error,omitempty"`
	LockedAt     *time.Time `json:"locked_at,omitempty"`
	SucceededAt  *time.Time `json:"succeeded_at,omitempty"`
	FailedAt     *time.Time `json:"failed_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type CustomReferralBindingDetail struct {
	ID            int64     `json:"id"`
	InviteeUserID int64     `json:"invitee_user_id"`
	InviteeEmail  string    `json:"invitee_email,omitempty"`
	InviteeName   string    `json:"invitee_name,omitempty"`
	BoundAt       time.Time `json:"bound_at"`
}

type CustomReferralRepository interface {
	UpsertApprovedAffiliate(ctx context.Context, userID, adminID int64, rateOverride *float64) (*CustomAffiliate, error)
	SetAffiliateRateOverride(ctx context.Context, userID int64, rateOverride *float64, audit CustomReferralAdminAuditContext) (*CustomAffiliate, error)
	SetAffiliateStatus(ctx context.Context, userID, adminID int64, status string, acquisitionEnabled, settlementEnabled, withdrawalEnabled bool, reason string, audit CustomReferralAdminAuditContext) (*CustomAffiliate, error)
	GetAffiliateByUserID(ctx context.Context, userID int64) (*CustomAffiliate, error)
	GetApprovedAffiliateByCode(ctx context.Context, code string) (*CustomAffiliate, error)
	RecordReferralClick(ctx context.Context, affiliateID int64, inviteCode string, click CustomReferralClickInput) error
	InviteeInInviterAncestorChain(ctx context.Context, inviteeUserID, inviterUserID int64, maxDepth int) (bool, error)
	BindInvitee(ctx context.Context, inviteeUserID, affiliateID, inviterUserID int64, bindSource, bindCode string, boundAt time.Time) (bool, error)
	SnapshotOrderAffiliate(ctx context.Context, userID int64, defaultRate float64) (*CustomReferralOrderSnapshot, error)
	CreatePendingCommissionForOrder(ctx context.Context, order CustomReferralOrderInput, freezeDays int) (float64, error)
	ReverseCommissionForRefund(ctx context.Context, refund CustomReferralRefundInput) (float64, error)
	ReverseCommissionManually(ctx context.Context, input CustomReferralManualReverseInput) (*CustomReferralCommissionReversal, error)
	RecordAdminAudit(ctx context.Context, targetUserID int64, audit CustomReferralAdminAuditContext) error
	SettleDueCommissions(ctx context.Context, now time.Time) error
	GetDashboardByUserID(ctx context.Context, userID int64) (*CustomReferralDashboard, error)
	UpsertAffiliateApplication(ctx context.Context, userID int64, note string) (*CustomAffiliate, error)
	ListAffiliates(ctx context.Context, params CustomReferralListParams) ([]CustomAffiliate, int64, error)
	ListAffiliateBindings(ctx context.Context, affiliateUserID int64, page, pageSize int) ([]CustomReferralBindingDetail, int64, error)
	GetAdminOverview(ctx context.Context) (*CustomReferralAdminOverview, error)
	AdjustAffiliateCommission(ctx context.Context, input CustomReferralAdjustInput) (*CustomAffiliate, error)
	ListCommissionsByUserID(ctx context.Context, userID int64, params CustomReferralCommissionListParams) ([]CustomReferralUserCommission, int64, error)
	ListCommissions(ctx context.Context, params CustomReferralCommissionListParams) ([]CustomReferralCommission, int64, error)
	ListAffiliateCommissions(ctx context.Context, affiliateUserID int64, params CustomReferralCommissionListParams) ([]CustomReferralCommission, int64, error)
	ListCommissionJobs(ctx context.Context, params CustomReferralCommissionListParams) ([]CustomReferralCommissionJob, int64, error)
	RunSettlementBatch(ctx context.Context, now time.Time) (*CustomReferralSettlementBatch, error)
	CreateWithdrawal(ctx context.Context, input CustomReferralWithdrawalCreateInput, feeAmount float64) (*CustomReferralWithdrawal, error)
	ListWithdrawalsByUserID(ctx context.Context, userID int64, params CustomReferralWithdrawalListParams) ([]CustomReferralWithdrawal, int64, error)
	ListWithdrawals(ctx context.Context, params CustomReferralWithdrawalListParams) ([]CustomReferralWithdrawal, int64, error)
	ListAffiliateWithdrawals(ctx context.Context, affiliateUserID int64, params CustomReferralWithdrawalListParams) ([]CustomReferralWithdrawal, int64, error)
	CancelWithdrawal(ctx context.Context, withdrawalID, userID int64) (*CustomReferralWithdrawal, error)
	ApproveWithdrawal(ctx context.Context, input CustomReferralWithdrawalReviewInput, deadlineAt time.Time) (*CustomReferralWithdrawal, error)
	RejectWithdrawal(ctx context.Context, input CustomReferralWithdrawalReviewInput) (*CustomReferralWithdrawal, error)
	MarkWithdrawalPaid(ctx context.Context, input CustomReferralWithdrawalPayInput) (*CustomReferralWithdrawal, error)
}

type CustomReferralService struct {
	repo        CustomReferralRepository
	settingRepo SettingRepository
	cfg         *config.Config
}

func NewCustomReferralService(repo CustomReferralRepository, settingRepo SettingRepository, cfg *config.Config) *CustomReferralService {
	return &CustomReferralService{
		repo:        repo,
		settingRepo: settingRepo,
		cfg:         cfg,
	}
}

func (s *CustomReferralService) IsEnabled(ctx context.Context) bool {
	cfg, err := s.loadConfig(ctx)
	if err != nil {
		return false
	}
	return cfg.Provider == CustomReferralProviderCustom
}

func (s *CustomReferralService) loadConfig(ctx context.Context) (*CustomReferralConfig, error) {
	if s == nil || s.settingRepo == nil {
		return &CustomReferralConfig{
			Provider:          CustomReferralProviderDisabled,
			CookieTTLDays:     customReferralDefaultCookieTTLDays,
			SettleFreezeDays:  customReferralDefaultFreezeDays,
			MinWithdrawAmount: 0,
			WithdrawFee:       0,
		}, nil
	}

	out := &CustomReferralConfig{
		Provider:          CustomReferralProviderDisabled,
		CookieTTLDays:     customReferralDefaultCookieTTLDays,
		SettleFreezeDays:  customReferralDefaultFreezeDays,
		MinWithdrawAmount: 0,
		WithdrawFee:       0,
	}

	if raw, err := s.settingRepo.GetValue(ctx, SettingKeyCustomReferralProvider); err == nil {
		provider := strings.ToLower(strings.TrimSpace(raw))
		switch provider {
		case CustomReferralProviderCustom, CustomReferralProviderDisabled:
			out.Provider = provider
		}
	}
	if raw, err := s.settingRepo.GetValue(ctx, SettingKeyCustomReferralCookieTTLDays); err == nil {
		if days, parseErr := strconv.Atoi(strings.TrimSpace(raw)); parseErr == nil && days > 0 {
			out.CookieTTLDays = days
		}
	}
	if raw, err := s.settingRepo.GetValue(ctx, SettingKeyCustomReferralSettleFreezeDays); err == nil {
		if days, parseErr := strconv.Atoi(strings.TrimSpace(raw)); parseErr == nil && days >= 0 {
			out.SettleFreezeDays = days
		}
	}
	if raw, err := s.settingRepo.GetValue(ctx, SettingKeyCustomReferralDefaultRate); err == nil {
		if rate, parseErr := strconv.ParseFloat(strings.TrimSpace(raw), 64); parseErr == nil && rate > 0 {
			out.DefaultRate = rate
			out.HasDefaultRate = true
		}
	}
	if raw, err := s.settingRepo.GetValue(ctx, SettingKeyCustomReferralMinWithdrawAmount); err == nil {
		if amount, parseErr := strconv.ParseFloat(strings.TrimSpace(raw), 64); parseErr == nil && amount >= 0 {
			out.MinWithdrawAmount = amount
		}
	}
	if raw, err := s.settingRepo.GetValue(ctx, SettingKeyCustomReferralWithdrawFee); err == nil {
		if fee, parseErr := strconv.ParseFloat(strings.TrimSpace(raw), 64); parseErr == nil && fee >= 0 {
			out.WithdrawFee = fee
		}
	}

	return out, nil
}

func (s *CustomReferralService) BuildSignedCookieValue(code string, issuedAt time.Time) (string, error) {
	code = strings.ToUpper(strings.TrimSpace(code))
	if code == "" {
		return "", fmt.Errorf("empty invite code")
	}
	secret := s.cookieSecret()
	if secret == "" {
		return "", fmt.Errorf("missing cookie signing secret")
	}
	payload := code + "." + strconv.FormatInt(issuedAt.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	signature := hex.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload + "." + signature)), nil
}

func (s *CustomReferralService) ParseSignedCookieValue(ctx context.Context, raw string) (string, error) {
	secret := s.cookieSecret()
	if secret == "" {
		return "", fmt.Errorf("missing cookie signing secret")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	parts := strings.Split(string(decoded), ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid cookie payload")
	}
	code := strings.ToUpper(strings.TrimSpace(parts[0]))
	payload := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return "", fmt.Errorf("invalid cookie signature")
	}
	issuedAt, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid cookie timestamp")
	}
	cfg := &CustomReferralConfig{CookieTTLDays: customReferralDefaultCookieTTLDays}
	if s != nil {
		if loaded, loadErr := s.loadConfig(ctx); loadErr == nil && loaded != nil {
			cfg = loaded
		}
	}
	issuedTime := time.Unix(issuedAt, 0)
	if issuedTime.After(time.Now().Add(5 * time.Minute)) {
		return "", fmt.Errorf("cookie issued_at is invalid")
	}
	if cfg.CookieTTLDays > 0 && issuedTime.Add(time.Duration(cfg.CookieTTLDays)*24*time.Hour).Before(time.Now()) {
		return "", fmt.Errorf("cookie expired")
	}
	return code, nil
}

func (s *CustomReferralService) HandleLanding(ctx context.Context, code string, click CustomReferralClickInput) (*CustomReferralLanding, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	if !s.IsEnabled(ctx) {
		return nil, ErrCustomReferralAffiliateDisabled
	}
	affiliate, err := s.repo.GetApprovedAffiliateByCode(ctx, code)
	if err != nil {
		return nil, err
	}
	if affiliate == nil || !affiliate.AcquisitionEnabled {
		return nil, ErrCustomReferralAffiliateDisabled
	}
	if click.ClickedAt.IsZero() {
		click.ClickedAt = time.Now()
	}
	if err := s.repo.RecordReferralClick(ctx, affiliate.ID, affiliate.InviteCode, click); err != nil {
		return nil, err
	}
	cfg, _ := s.loadConfig(ctx)
	return &CustomReferralLanding{
		Code:          affiliate.InviteCode,
		RedirectPath:  customReferralDefaultRedirectPath,
		CookieTTLDays: cfg.CookieTTLDays,
	}, nil
}

func (s *CustomReferralService) BindInviteeByCode(ctx context.Context, inviteeUserID int64, code string) error {
	if inviteeUserID <= 0 {
		return nil
	}
	code = strings.ToUpper(strings.TrimSpace(code))
	if code == "" || s == nil || s.repo == nil || !s.IsEnabled(ctx) {
		return nil
	}

	affiliate, err := s.repo.GetApprovedAffiliateByCode(ctx, code)
	if err != nil {
		return err
	}
	if affiliate == nil {
		return ErrCustomReferralAffiliateNotFound
	}
	if affiliate.UserID == inviteeUserID {
		return ErrCustomReferralSelfInvite
	}
	if !affiliate.AcquisitionEnabled {
		return ErrCustomReferralAffiliateDisabled
	}
	cyclic, err := s.repo.InviteeInInviterAncestorChain(ctx, inviteeUserID, affiliate.UserID, customReferralMaxAncestorDepth)
	if err != nil {
		return err
	}
	if cyclic {
		return ErrCustomReferralCycleInvite
	}
	bound, err := s.repo.BindInvitee(ctx, inviteeUserID, affiliate.ID, affiliate.UserID, "cookie", affiliate.InviteCode, time.Now())
	if err != nil {
		return err
	}
	if !bound {
		return ErrCustomReferralAlreadyBound
	}
	return nil
}

func (s *CustomReferralService) ValidateInviteCodeForSignup(ctx context.Context, code string) error {
	code = strings.ToUpper(strings.TrimSpace(code))
	if code == "" {
		return nil
	}
	if s == nil || s.repo == nil {
		return ErrServiceUnavailable
	}
	if !s.IsEnabled(ctx) {
		return ErrCustomReferralAffiliateDisabled
	}
	affiliate, err := s.repo.GetApprovedAffiliateByCode(ctx, code)
	if err != nil {
		return err
	}
	if affiliate == nil || !affiliate.AcquisitionEnabled {
		return ErrCustomReferralAffiliateDisabled
	}
	return nil
}

func (s *CustomReferralService) CreateCommissionForOrder(ctx context.Context, order CustomReferralOrderInput) (float64, error) {
	if order.OrderID <= 0 || order.UserID <= 0 || order.AffiliateID <= 0 || order.BaseAmount <= 0 || order.Rate <= 0 {
		return 0, nil
	}
	if s == nil || s.repo == nil || !s.IsEnabled(ctx) {
		return 0, nil
	}
	cfg, err := s.loadConfig(ctx)
	if err != nil {
		return 0, err
	}
	return s.repo.CreatePendingCommissionForOrder(ctx, order, cfg.SettleFreezeDays)
}

func (s *CustomReferralService) SnapshotOrderAffiliate(ctx context.Context, userID int64) (*CustomReferralOrderSnapshot, error) {
	if userID <= 0 || s == nil || s.repo == nil {
		return nil, nil
	}
	cfg, err := s.loadConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg.Provider != CustomReferralProviderCustom {
		return nil, nil
	}
	if !cfg.HasDefaultRate {
		return nil, ErrCustomReferralRateNotConfigured
	}
	return s.repo.SnapshotOrderAffiliate(ctx, userID, cfg.DefaultRate)
}

func (s *CustomReferralService) ReverseCommissionForRefund(ctx context.Context, refund CustomReferralRefundInput) (float64, error) {
	if refund.OrderID <= 0 || refund.RefundAmount <= 0 || s == nil || s.repo == nil {
		return 0, nil
	}
	return s.repo.ReverseCommissionForRefund(ctx, refund)
}

func (s *CustomReferralService) GetDashboard(ctx context.Context, userID int64) (*CustomReferralDashboard, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, err
	}
	dashboard, err := s.repo.GetDashboardByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if dashboard == nil || dashboard.Status == "" {
		return nil, ErrCustomReferralPermissionDenied
	}
	cfg, cfgErr := s.loadConfig(ctx)
	if cfgErr == nil && cfg != nil {
		dashboard.MinWithdrawAmount = cfg.MinWithdrawAmount
		if dashboard.Rate == nil && cfg.HasDefaultRate {
			rate := cfg.DefaultRate
			dashboard.Rate = &rate
		}
	}
	return dashboard, nil
}

func (s *CustomReferralService) GetProfile(ctx context.Context, userID int64) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.GetAffiliateByUserID(ctx, userID)
	if err != nil {
		if err == ErrCustomReferralAffiliateNotFound {
			return nil, nil
		}
		return nil, err
	}
	return item, nil
}

func (s *CustomReferralService) ApplyAffiliate(ctx context.Context, userID int64, note string) (*CustomAffiliate, error) {
	if s == nil {
		return nil, ErrServiceUnavailable
	}
	if !s.IsEnabled(ctx) {
		return nil, ErrCustomReferralAffiliateDisabled
	}
	if s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	return s.repo.UpsertAffiliateApplication(ctx, userID, strings.TrimSpace(note))
}

func (s *CustomReferralService) ApproveAffiliate(ctx context.Context, userID, adminID int64, rateOverride *float64) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	return s.repo.UpsertApprovedAffiliate(ctx, userID, adminID, normalizeRatePointer(rateOverride))
}

func (s *CustomReferralService) SetAffiliateRateOverride(ctx context.Context, userID int64, rateOverride *float64, audit CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	if strings.TrimSpace(audit.Action) == "" {
		audit.Action = "affiliate_rate_override"
	}
	return s.repo.SetAffiliateRateOverride(ctx, userID, normalizeRatePointer(rateOverride), audit)
}

func (s *CustomReferralService) DisableAffiliate(ctx context.Context, userID, adminID int64, reason string, auditCtx ...CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	return s.repo.SetAffiliateStatus(ctx, userID, adminID, CustomAffiliateStatusDisabled, false, false, false, strings.TrimSpace(reason), customReferralAuditContext("affiliate_disable", adminID, reason, auditCtx...))
}

func (s *CustomReferralService) RejectAffiliate(ctx context.Context, userID, adminID int64, reason string, auditCtx ...CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	return s.repo.SetAffiliateStatus(ctx, userID, adminID, CustomAffiliateStatusRejected, false, false, false, strings.TrimSpace(reason), customReferralAuditContext("affiliate_reject", adminID, reason, auditCtx...))
}

func (s *CustomReferralService) RestoreAffiliate(ctx context.Context, userID, adminID int64, auditCtx ...CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	return s.repo.SetAffiliateStatus(ctx, userID, adminID, CustomAffiliateStatusApproved, true, true, true, "", customReferralAuditContext("affiliate_restore", adminID, "", auditCtx...))
}

func (s *CustomReferralService) FreezeSettlement(ctx context.Context, userID, adminID int64, reason string, auditCtx ...CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.GetAffiliateByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	return s.repo.SetAffiliateStatus(ctx, userID, adminID, item.Status, item.AcquisitionEnabled, false, item.WithdrawalEnabled, strings.TrimSpace(reason), customReferralAuditContext("settlement_freeze", adminID, reason, auditCtx...))
}

func (s *CustomReferralService) RestoreSettlement(ctx context.Context, userID, adminID int64, auditCtx ...CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.GetAffiliateByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	return s.repo.SetAffiliateStatus(ctx, userID, adminID, item.Status, item.AcquisitionEnabled, true, item.WithdrawalEnabled, item.RiskReason, customReferralAuditContext("settlement_restore", adminID, item.RiskReason, auditCtx...))
}

func (s *CustomReferralService) FreezeWithdrawal(ctx context.Context, userID, adminID int64, reason string, auditCtx ...CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.GetAffiliateByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	return s.repo.SetAffiliateStatus(ctx, userID, adminID, item.Status, item.AcquisitionEnabled, item.SettlementEnabled, false, strings.TrimSpace(reason), customReferralAuditContext("withdrawal_freeze", adminID, reason, auditCtx...))
}

func (s *CustomReferralService) RestoreWithdrawal(ctx context.Context, userID, adminID int64, auditCtx ...CustomReferralAdminAuditContext) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.GetAffiliateByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	return s.repo.SetAffiliateStatus(ctx, userID, adminID, item.Status, item.AcquisitionEnabled, item.SettlementEnabled, true, item.RiskReason, customReferralAuditContext("withdrawal_restore", adminID, item.RiskReason, auditCtx...))
}

func (s *CustomReferralService) ListAffiliates(ctx context.Context, params CustomReferralListParams) ([]CustomAffiliate, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, 0, err
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	return s.repo.ListAffiliates(ctx, params)
}

func (s *CustomReferralService) ListAffiliateBindings(ctx context.Context, affiliateUserID int64, page, pageSize int) ([]CustomReferralBindingDetail, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	return s.repo.ListAffiliateBindings(ctx, affiliateUserID, page, pageSize)
}

func (s *CustomReferralService) GetAdminOverview(ctx context.Context) (*CustomReferralAdminOverview, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, err
	}
	return s.repo.GetAdminOverview(ctx)
}

func (s *CustomReferralService) GetConfig(ctx context.Context) (*CustomReferralAdminConfig, error) {
	cfg, err := s.loadConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &CustomReferralAdminConfig{
		Provider:          cfg.Provider,
		CookieTTLDays:     cfg.CookieTTLDays,
		DefaultRate:       cfg.DefaultRate,
		HasDefaultRate:    cfg.HasDefaultRate,
		SettleFreezeDays:  cfg.SettleFreezeDays,
		MinWithdrawAmount: cfg.MinWithdrawAmount,
		WithdrawFee:       cfg.WithdrawFee,
	}, nil
}

func (s *CustomReferralService) UpdateConfig(ctx context.Context, input CustomReferralAdminConfig, auditCtx ...CustomReferralAdminAuditContext) (*CustomReferralAdminConfig, error) {
	if s == nil || s.settingRepo == nil {
		return nil, ErrServiceUnavailable
	}
	oldConfig, _ := s.GetConfig(ctx)

	provider := strings.ToLower(strings.TrimSpace(input.Provider))
	switch provider {
	case "", CustomReferralProviderDisabled:
		provider = CustomReferralProviderDisabled
	case CustomReferralProviderCustom:
	default:
		return nil, infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_PROVIDER", "invalid referral provider")
	}

	if input.CookieTTLDays < 0 {
		return nil, infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_COOKIE_TTL", "cookie ttl must be non-negative")
	}
	if input.SettleFreezeDays < 0 {
		return nil, infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_SETTLE_FREEZE_DAYS", "settle freeze days must be non-negative")
	}
	if input.DefaultRate < 0 || input.DefaultRate > 100 {
		return nil, infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_DEFAULT_RATE", "default rate must be between 0 and 100")
	}
	if provider == CustomReferralProviderCustom && input.DefaultRate <= 0 {
		return nil, ErrCustomReferralRateNotConfigured
	}
	if input.MinWithdrawAmount < 0 {
		return nil, infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_MIN_WITHDRAW", "minimum withdraw amount must be non-negative")
	}
	if input.WithdrawFee < 0 {
		return nil, infraerrors.BadRequest("CUSTOM_REFERRAL_INVALID_WITHDRAW_FEE", "withdraw fee must be non-negative")
	}

	updates := map[string]string{
		SettingKeyCustomReferralProvider:          provider,
		SettingKeyCustomReferralCookieTTLDays:     strconv.Itoa(input.CookieTTLDays),
		SettingKeyCustomReferralDefaultRate:       strconv.FormatFloat(input.DefaultRate, 'f', 4, 64),
		SettingKeyCustomReferralSettleFreezeDays:  strconv.Itoa(input.SettleFreezeDays),
		SettingKeyCustomReferralMinWithdrawAmount: strconv.FormatFloat(input.MinWithdrawAmount, 'f', 2, 64),
		SettingKeyCustomReferralWithdrawFee:       strconv.FormatFloat(input.WithdrawFee, 'f', 2, 64),
	}
	if err := s.settingRepo.SetMultiple(ctx, updates); err != nil {
		return nil, err
	}
	updatedConfig, err := s.GetConfig(ctx)
	if err != nil {
		return nil, err
	}
	if len(auditCtx) > 0 && s.repo != nil {
		audit := auditCtx[0]
		if strings.TrimSpace(audit.Action) == "" {
			audit.Action = "referral_settings_update"
		}
		if audit.OldValue == nil {
			audit.OldValue = customReferralAdminConfigAuditValue(oldConfig)
		}
		if audit.NewValue == nil {
			audit.NewValue = customReferralAdminConfigAuditValue(updatedConfig)
		}
		if err := s.repo.RecordAdminAudit(ctx, 0, audit); err != nil {
			return nil, err
		}
	}
	return updatedConfig, nil
}

func customReferralAdminConfigAuditValue(cfg *CustomReferralAdminConfig) map[string]any {
	if cfg == nil {
		return map[string]any{}
	}
	return map[string]any{
		"provider":            cfg.Provider,
		"cookie_ttl_days":     cfg.CookieTTLDays,
		"default_rate":        moneyx.Rate(cfg.DefaultRate).InexactFloat64(),
		"has_default_rate":    cfg.HasDefaultRate,
		"settle_freeze_days":  cfg.SettleFreezeDays,
		"min_withdraw_amount": moneyx.Commission(cfg.MinWithdrawAmount).InexactFloat64(),
		"withdraw_fee":        moneyx.Commission(cfg.WithdrawFee).InexactFloat64(),
	}
}

func customReferralAuditContext(action string, adminID int64, reason string, provided ...CustomReferralAdminAuditContext) CustomReferralAdminAuditContext {
	var audit CustomReferralAdminAuditContext
	if len(provided) > 0 {
		audit = provided[0]
	}
	if strings.TrimSpace(audit.Action) == "" {
		audit.Action = action
	}
	if audit.AdminUserID <= 0 {
		audit.AdminUserID = adminID
	}
	if strings.TrimSpace(audit.Reason) == "" {
		audit.Reason = strings.TrimSpace(reason)
	}
	return audit
}

func (s *CustomReferralService) ListUserCommissions(ctx context.Context, userID int64, params CustomReferralCommissionListParams) ([]CustomReferralUserCommission, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, 0, err
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	return s.repo.ListCommissionsByUserID(ctx, userID, params)
}

func (s *CustomReferralService) ListCommissions(ctx context.Context, params CustomReferralCommissionListParams) ([]CustomReferralCommission, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, 0, err
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	return s.repo.ListCommissions(ctx, params)
}

func (s *CustomReferralService) ListAffiliateCommissions(ctx context.Context, affiliateUserID int64, params CustomReferralCommissionListParams) ([]CustomReferralCommission, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, 0, err
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	return s.repo.ListAffiliateCommissions(ctx, affiliateUserID, params)
}

func (s *CustomReferralService) ListCommissionJobs(ctx context.Context, params CustomReferralCommissionListParams) ([]CustomReferralCommissionJob, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	return s.repo.ListCommissionJobs(ctx, params)
}

func (s *CustomReferralService) RunSettlementBatch(ctx context.Context) (*CustomReferralSettlementBatch, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	return s.repo.RunSettlementBatch(ctx, time.Now())
}

func (s *CustomReferralService) CreateWithdrawal(ctx context.Context, input CustomReferralWithdrawalCreateInput) (*CustomReferralWithdrawal, error) {
	if s == nil {
		return nil, ErrServiceUnavailable
	}
	if !s.IsEnabled(ctx) {
		return nil, ErrCustomReferralWithdrawDisabled
	}
	if s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, err
	}
	cfg, err := s.loadConfig(ctx)
	if err != nil {
		return nil, err
	}
	if !moneyx.Commission(input.Amount).GreaterThan(moneyx.Commission(0)) {
		return nil, ErrCustomReferralWithdrawTooSmall
	}
	if moneyx.Commission(cfg.MinWithdrawAmount).GreaterThan(moneyx.Commission(0)) &&
		moneyx.Commission(input.Amount).LessThan(moneyx.Commission(cfg.MinWithdrawAmount)) {
		return nil, ErrCustomReferralWithdrawTooSmall
	}
	input.AccountType = strings.ToLower(strings.TrimSpace(input.AccountType))
	input.AccountNo = strings.TrimSpace(input.AccountNo)
	input.AccountNetwork = strings.TrimSpace(input.AccountNetwork)
	input.AccountName = strings.TrimSpace(input.AccountName)
	input.QRImageURL = NormalizeReferralAssetURL(input.QRImageURL)
	input.ContactInfo = ""
	input.ApplicantNote = strings.TrimSpace(input.ApplicantNote)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.IdempotencyKey == "" || len(input.IdempotencyKey) > 128 {
		return nil, ErrCustomReferralInvalidIdempotency
	}
	if input.QRImageURL != "" && !IsReferralAssetPublicPath(input.QRImageURL) {
		return nil, ErrCustomReferralInvalidAsset
	}
	if input.AccountNo == "" {
		return nil, ErrCustomReferralWithdrawAccountEmpty
	}
	switch input.AccountType {
	case "alipay", "wechat":
		input.AccountNetwork = ""
	case "usdt":
		switch strings.ToUpper(input.AccountNetwork) {
		case "TRC20", "BEP20", "POLYGON":
			if strings.EqualFold(input.AccountNetwork, "Polygon") {
				input.AccountNetwork = "Polygon"
			} else {
				input.AccountNetwork = strings.ToUpper(input.AccountNetwork)
			}
			input.AccountName = ""
		default:
			return nil, ErrCustomReferralInvalidWithdrawNet
		}
	default:
		return nil, ErrCustomReferralInvalidWithdrawType
	}
	item, err := s.repo.CreateWithdrawal(ctx, input, cfg.WithdrawFee)
	if err != nil {
		return nil, err
	}
	s.signWithdrawalAssetURLs(item)
	return item, nil
}

func (s *CustomReferralService) ListUserWithdrawals(ctx context.Context, userID int64, params CustomReferralWithdrawalListParams) ([]CustomReferralWithdrawal, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, 0, err
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	items, total, err := s.repo.ListWithdrawalsByUserID(ctx, userID, params)
	if err != nil {
		return nil, 0, err
	}
	s.signWithdrawalAssetURLList(items)
	return items, total, nil
}

func (s *CustomReferralService) ListWithdrawals(ctx context.Context, params CustomReferralWithdrawalListParams) ([]CustomReferralWithdrawal, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, 0, err
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	items, total, err := s.repo.ListWithdrawals(ctx, params)
	if err != nil {
		return nil, 0, err
	}
	s.signWithdrawalAssetURLList(items)
	return items, total, nil
}

func (s *CustomReferralService) ListAffiliateWithdrawals(ctx context.Context, affiliateUserID int64, params CustomReferralWithdrawalListParams) ([]CustomReferralWithdrawal, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, ErrServiceUnavailable
	}
	if err := s.settleDueCommissions(ctx); err != nil {
		return nil, 0, err
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	if params.PageSize <= 0 {
		params.PageSize = 20
	}
	items, total, err := s.repo.ListAffiliateWithdrawals(ctx, affiliateUserID, params)
	if err != nil {
		return nil, 0, err
	}
	s.signWithdrawalAssetURLList(items)
	return items, total, nil
}

func (s *CustomReferralService) AdjustAffiliateCommission(ctx context.Context, input CustomReferralAdjustInput) (*CustomAffiliate, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	input.Remark = strings.TrimSpace(input.Remark)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.DeltaDecimal.IsZero() {
		input.DeltaDecimal = moneyx.Commission(input.Delta)
	} else {
		input.DeltaDecimal = input.DeltaDecimal.Round(moneyx.ScaleCommission)
	}
	if input.DeltaDecimal.IsZero() {
		return nil, infraerrors.BadRequest("CUSTOM_REFERRAL_ADJUST_ZERO", "adjust amount must not be zero")
	}
	if input.IdempotencyKey == "" || len(input.IdempotencyKey) > 128 {
		return nil, ErrCustomReferralInvalidIdempotency
	}
	if input.Audit.AdminUserID <= 0 {
		input.Audit.AdminUserID = input.AdminUserID
	}
	if strings.TrimSpace(input.Audit.Action) == "" {
		input.Audit.Action = "affiliate_commission_adjust"
	}
	if strings.TrimSpace(input.Audit.Reason) == "" {
		input.Audit.Reason = input.Remark
	}
	return s.repo.AdjustAffiliateCommission(ctx, input)
}

func (s *CustomReferralService) ReverseCommissionManually(ctx context.Context, input CustomReferralManualReverseInput) (*CustomReferralCommissionReversal, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	input.Reason = strings.TrimSpace(input.Reason)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.OrderID <= 0 && input.CommissionID <= 0 {
		return nil, ErrCustomReferralInvalidReverseInput
	}
	if input.RefundAmount <= 0 {
		return nil, ErrCustomReferralInvalidReverseInput
	}
	if input.IdempotencyKey == "" {
		return nil, ErrCustomReferralInvalidIdempotency
	}
	if input.Reason == "" {
		return nil, ErrCustomReferralReverseReasonRequired
	}
	if input.ReversedAt.IsZero() {
		input.ReversedAt = time.Now()
	}
	return s.repo.ReverseCommissionManually(ctx, input)
}

func (s *CustomReferralService) CancelWithdrawal(ctx context.Context, withdrawalID, userID int64) (*CustomReferralWithdrawal, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.CancelWithdrawal(ctx, withdrawalID, userID)
	if err != nil {
		return nil, err
	}
	s.signWithdrawalAssetURLs(item)
	return item, nil
}

func (s *CustomReferralService) ApproveWithdrawal(ctx context.Context, input CustomReferralWithdrawalReviewInput) (*CustomReferralWithdrawal, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.ApproveWithdrawal(ctx, input, time.Now().Add(48*time.Hour))
	if err != nil {
		return nil, err
	}
	s.signWithdrawalAssetURLs(item)
	return item, nil
}

func (s *CustomReferralService) RejectWithdrawal(ctx context.Context, input CustomReferralWithdrawalReviewInput) (*CustomReferralWithdrawal, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	item, err := s.repo.RejectWithdrawal(ctx, input)
	if err != nil {
		return nil, err
	}
	s.signWithdrawalAssetURLs(item)
	return item, nil
}

func (s *CustomReferralService) MarkWithdrawalPaid(ctx context.Context, input CustomReferralWithdrawalPayInput) (*CustomReferralWithdrawal, error) {
	if s == nil || s.repo == nil {
		return nil, ErrServiceUnavailable
	}
	input.PaymentProofURL = NormalizeReferralAssetURL(input.PaymentProofURL)
	if input.PaymentProofURL != "" && !IsReferralAssetPublicPath(input.PaymentProofURL) {
		return nil, ErrCustomReferralInvalidAsset
	}
	item, err := s.repo.MarkWithdrawalPaid(ctx, input)
	if err != nil {
		return nil, err
	}
	s.signWithdrawalAssetURLs(item)
	return item, nil
}

func (s *CustomReferralService) settleDueCommissions(ctx context.Context) error {
	if s == nil || s.repo == nil || !s.IsEnabled(ctx) {
		return nil
	}
	return s.repo.SettleDueCommissions(ctx, time.Now())
}

func (s *CustomReferralService) cookieSecret() string {
	if s == nil || s.cfg == nil {
		return ""
	}
	return strings.TrimSpace(s.cfg.JWT.Secret)
}

func (s *CustomReferralService) SignReferralAssetURL(raw string, now time.Time) (string, error) {
	path := NormalizeReferralAssetURL(raw)
	if path == "" {
		return "", nil
	}
	if !IsReferralAssetPublicPath(path) {
		return "", ErrCustomReferralInvalidAsset
	}
	secret := s.cookieSecret()
	if secret == "" {
		return "", fmt.Errorf("missing referral asset signing secret")
	}
	expires := now.Add(15 * time.Minute).Unix()
	payload := path + "." + strconv.FormatInt(expires, 10)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	signature := hex.EncodeToString(mac.Sum(nil))
	return path + "?expires=" + strconv.FormatInt(expires, 10) + "&sig=" + signature, nil
}

func (s *CustomReferralService) VerifyReferralAssetURL(path, expiresRaw, signature string, now time.Time) bool {
	path = NormalizeReferralAssetURL(path)
	if path == "" || !IsReferralAssetPublicPath(path) {
		return false
	}
	expires, err := strconv.ParseInt(strings.TrimSpace(expiresRaw), 10, 64)
	if err != nil || expires <= 0 || now.Unix() > expires {
		return false
	}
	secret := s.cookieSecret()
	if secret == "" {
		return false
	}
	payload := path + "." + strconv.FormatInt(expires, 10)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(strings.TrimSpace(signature)))
}

func HashCustomReferralRiskValue(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("custom_referral:" + raw))
	return hex.EncodeToString(sum[:])
}

func (s *CustomReferralService) signWithdrawalAssetURLList(items []CustomReferralWithdrawal) {
	for i := range items {
		s.signWithdrawalAssetURLs(&items[i])
	}
}

func (s *CustomReferralService) signWithdrawalAssetURLs(item *CustomReferralWithdrawal) {
	if item == nil {
		return
	}
	item.QRImageURL = s.signReferralAssetField(item.QRImageURL)
	item.PaymentProofURL = s.signReferralAssetField(item.PaymentProofURL)
}

func (s *CustomReferralService) signReferralAssetField(raw string) string {
	path := NormalizeReferralAssetURL(raw)
	if path == "" || !IsReferralAssetPublicPath(path) {
		return ""
	}
	signed, err := s.SignReferralAssetURL(path, time.Now())
	if err != nil {
		return ""
	}
	return signed
}

func normalizeRatePointer(rate *float64) *float64 {
	if rate == nil {
		return nil
	}
	value := *rate
	if value < 0 {
		value = 0
	}
	if value > 100 {
		value = 100
	}
	return &value
}
