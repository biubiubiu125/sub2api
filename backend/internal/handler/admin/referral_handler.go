package admin

import (
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

type ReferralHandler struct {
	referralService *service.CustomReferralService
	assetService    *service.ReferralAssetService
}

func NewReferralHandler(referralService *service.CustomReferralService, assetService *service.ReferralAssetService) *ReferralHandler {
	return &ReferralHandler{
		referralService: referralService,
		assetService:    assetService,
	}
}

type approveAffiliateRequest struct {
	RateOverride *float64 `json:"rate_override,omitempty"`
}

type disableAffiliateRequest struct {
	Reason string `json:"reason"`
}

type reviewWithdrawalRequest struct {
	AdminNote    string `json:"admin_note"`
	RejectReason string `json:"reject_reason"`
}

type payWithdrawalRequest struct {
	AdminNote       string `json:"admin_note"`
	PaymentProofURL string `json:"payment_proof_url"`
	PaymentTxnNo    string `json:"payment_txn_no"`
}

type updateReferralSettingsRequest struct {
	Provider          string  `json:"provider"`
	CookieTTLDays     int     `json:"cookie_ttl_days"`
	DefaultRate       float64 `json:"default_rate"`
	SettleFreezeDays  int     `json:"settle_freeze_days"`
	MinWithdrawAmount float64 `json:"min_withdraw_amount"`
	WithdrawFee       float64 `json:"withdraw_fee"`
}

func (h *ReferralHandler) Overview(c *gin.Context) {
	overview, err := h.referralService.GetAdminOverview(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, overview)
}

func (h *ReferralHandler) GetSettings(c *gin.Context) {
	config, err := h.referralService.GetConfig(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, config)
}

func (h *ReferralHandler) UpdateSettings(c *gin.Context) {
	var req updateReferralSettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	config, err := h.referralService.UpdateConfig(c.Request.Context(), service.CustomReferralAdminConfig{
		Provider:          strings.TrimSpace(req.Provider),
		CookieTTLDays:     req.CookieTTLDays,
		DefaultRate:       req.DefaultRate,
		SettleFreezeDays:  req.SettleFreezeDays,
		MinWithdrawAmount: req.MinWithdrawAmount,
		WithdrawFee:       req.WithdrawFee,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, config)
}

func (h *ReferralHandler) ListAffiliates(c *gin.Context) {
	page, pageSize := response.ParsePagination(c)
	items, total, err := h.referralService.ListAffiliates(c.Request.Context(), service.CustomReferralListParams{
		Page:     page,
		PageSize: pageSize,
		Status:   strings.TrimSpace(c.Query("status")),
		Keyword:  strings.TrimSpace(c.Query("keyword")),
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Paginated(c, items, total, page, pageSize)
}

func (h *ReferralHandler) ApproveAffiliate(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req approveAffiliateRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.ApproveAffiliate(c.Request.Context(), userID, subject.UserID, req.RateOverride)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) DisableAffiliate(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req disableAffiliateRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.DisableAffiliate(c.Request.Context(), userID, subject.UserID, req.Reason)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) RejectAffiliate(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req disableAffiliateRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.RejectAffiliate(c.Request.Context(), userID, subject.UserID, req.Reason)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) RestoreAffiliate(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	item, err := h.referralService.RestoreAffiliate(c.Request.Context(), userID, subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) FreezeSettlement(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req disableAffiliateRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.FreezeSettlement(c.Request.Context(), userID, subject.UserID, req.Reason)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) RestoreSettlement(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	item, err := h.referralService.RestoreSettlement(c.Request.Context(), userID, subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) FreezeWithdrawal(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req disableAffiliateRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.FreezeWithdrawal(c.Request.Context(), userID, subject.UserID, req.Reason)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) RestoreWithdrawal(c *gin.Context) {
	userID, ok := parseReferralUserID(c)
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	item, err := h.referralService.RestoreWithdrawal(c.Request.Context(), userID, subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) RunSettlementBatch(c *gin.Context) {
	result, err := h.referralService.RunSettlementBatch(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *ReferralHandler) ListCommissions(c *gin.Context) {
	page, pageSize := response.ParsePagination(c)
	items, total, err := h.referralService.ListCommissions(c.Request.Context(), service.CustomReferralCommissionListParams{
		Page:     page,
		PageSize: pageSize,
		Status:   strings.TrimSpace(c.Query("status")),
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Paginated(c, items, total, page, pageSize)
}

func (h *ReferralHandler) ListWithdrawals(c *gin.Context) {
	page, pageSize := response.ParsePagination(c)
	items, total, err := h.referralService.ListWithdrawals(c.Request.Context(), service.CustomReferralWithdrawalListParams{
		Page:     page,
		PageSize: pageSize,
		Status:   strings.TrimSpace(c.Query("status")),
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Paginated(c, items, total, page, pageSize)
}

func (h *ReferralHandler) ApproveWithdrawal(c *gin.Context) {
	withdrawalID, ok := parseReferralRecordID(c, "id")
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req reviewWithdrawalRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.ApproveWithdrawal(c.Request.Context(), service.CustomReferralWithdrawalReviewInput{
		WithdrawalID: withdrawalID,
		AdminUserID:  subject.UserID,
		AdminNote:    strings.TrimSpace(req.AdminNote),
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) RejectWithdrawal(c *gin.Context) {
	withdrawalID, ok := parseReferralRecordID(c, "id")
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req reviewWithdrawalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.RejectWithdrawal(c.Request.Context(), service.CustomReferralWithdrawalReviewInput{
		WithdrawalID: withdrawalID,
		AdminUserID:  subject.UserID,
		AdminNote:    strings.TrimSpace(req.AdminNote),
		RejectReason: strings.TrimSpace(req.RejectReason),
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) MarkWithdrawalPaid(c *gin.Context) {
	withdrawalID, ok := parseReferralRecordID(c, "id")
	if !ok {
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "Unauthorized")
		return
	}
	var req payWithdrawalRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.MarkWithdrawalPaid(c.Request.Context(), service.CustomReferralWithdrawalPayInput{
		WithdrawalID:    withdrawalID,
		AdminUserID:     subject.UserID,
		AdminNote:       strings.TrimSpace(req.AdminNote),
		PaymentProofURL: strings.TrimSpace(req.PaymentProofURL),
		PaymentTxnNo:    strings.TrimSpace(req.PaymentTxnNo),
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) UploadAsset(c *gin.Context) {
	if h == nil || h.assetService == nil {
		response.InternalError(c, "asset service unavailable")
		return
	}
	fileHeader, err := c.FormFile("file")
	if err != nil {
		response.BadRequest(c, "file is required")
		return
	}
	file, err := fileHeader.Open()
	if err != nil {
		response.InternalError(c, "failed to open upload")
		return
	}
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	if err != nil {
		response.InternalError(c, "failed to read upload")
		return
	}
	contentType := http.DetectContentType(data)
	url, err := h.assetService.SaveImage("payment-proof", data, contentType)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"url": url})
}

func parseReferralUserID(c *gin.Context) (int64, bool) {
	userID, err := strconv.ParseInt(strings.TrimSpace(c.Param("user_id")), 10, 64)
	if err != nil || userID <= 0 {
		response.BadRequest(c, "invalid user_id")
		return 0, false
	}
	return userID, true
}

func parseReferralRecordID(c *gin.Context, key string) (int64, bool) {
	value, err := strconv.ParseInt(strings.TrimSpace(c.Param(key)), 10, 64)
	if err != nil || value <= 0 {
		response.BadRequest(c, "invalid id")
		return 0, false
	}
	return value, true
}
