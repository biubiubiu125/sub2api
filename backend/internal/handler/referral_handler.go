package handler

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	ratelimit "github.com/Wei-Shaw/sub2api/internal/middleware"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

type ReferralHandler struct {
	referralService *service.CustomReferralService
	assetService    *service.ReferralAssetService
	landingLimiter  *ratelimit.RateLimiter
	settingService  *service.SettingService
}

type createReferralWithdrawalRequest struct {
	Amount         float64 `json:"amount"`
	AccountType    string  `json:"account_type"`
	AccountName    string  `json:"account_name"`
	AccountNo      string  `json:"account_no"`
	AccountNetwork string  `json:"account_network"`
	QRImageURL     string  `json:"qr_image_url"`
	ContactInfo    string  `json:"contact_info"`
	ApplicantNote  string  `json:"applicant_note"`
	IdempotencyKey string  `json:"idempotency_key"`
}

type applyReferralRequest struct {
	ApplicantNote string `json:"applicant_note"`
}

func NewReferralHandler(referralService *service.CustomReferralService, assetService *service.ReferralAssetService) *ReferralHandler {
	return &ReferralHandler{
		referralService: referralService,
		assetService:    assetService,
	}
}

func (h *ReferralHandler) SetLandingRateLimit(rateLimiter *ratelimit.RateLimiter, settingService *service.SettingService) {
	if h == nil {
		return
	}
	h.landingLimiter = rateLimiter
	h.settingService = settingService
}

func (h *ReferralHandler) CaptureReferral(c *gin.Context) {
	if h == nil || h.referralService == nil {
		c.Redirect(http.StatusFound, "/register")
		return
	}
	if err := h.enforceLandingRateLimit(c, c.Param("code")); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	landing, err := h.referralService.HandleLanding(
		c.Request.Context(),
		c.Param("code"),
		service.CustomReferralClickInput{
			Referer:       c.GetHeader("Referer"),
			LandingPath:   c.Request.URL.Path,
			IPHash:        service.HashCustomReferralRiskValue(c.ClientIP()),
			UserAgentHash: service.HashCustomReferralRiskValue(c.GetHeader("User-Agent")),
			ClickedAt:     time.Now(),
		},
	)
	if err == nil && landing != nil {
		h.setReferralCookie(c, landing)
		c.Redirect(http.StatusFound, referralRegisterRedirect(landing.RedirectPath, landing.Code))
		return
	}
	c.Redirect(http.StatusFound, referralRegisterErrorRedirect())
}

func (h *ReferralHandler) enforceLandingRateLimit(c *gin.Context, rawCode string) error {
	if h == nil || h.landingLimiter == nil || c == nil {
		return nil
	}
	settings := service.DefaultReferralLandingRateLimitSettings()
	if h.settingService != nil {
		settings = h.settingService.GetReferralLandingRateLimitSettings(c.Request.Context())
	}
	if !settings.Enabled {
		return nil
	}

	if blocked, err := h.landingLimiter.CheckWithOptions(
		c.Request.Context(),
		"custom-referral-landing:ip:"+strings.TrimSpace(c.ClientIP()),
		settings.PerIPPerMinute,
		time.Minute,
		ratelimit.RateLimitOptions{FailureMode: ratelimit.RateLimitFailClose},
	); blocked {
		return registerRateLimitError("landing_ip", err)
	}

	code := strings.ToUpper(strings.TrimSpace(rawCode))
	if blocked, err := h.landingLimiter.CheckWithOptions(
		c.Request.Context(),
		"custom-referral-landing:invite-code:"+code,
		settings.PerInviteCodePerMinute,
		time.Minute,
		ratelimit.RateLimitOptions{FailureMode: ratelimit.RateLimitFailClose},
	); blocked {
		return registerRateLimitError("landing_invite_code", err)
	}

	return nil
}

func (h *ReferralHandler) setReferralCookie(c *gin.Context, landing *service.CustomReferralLanding) {
	if h == nil || h.referralService == nil || c == nil || landing == nil || strings.TrimSpace(landing.Code) == "" {
		return
	}
	signed, err := h.referralService.BuildSignedCookieValue(landing.Code, time.Now())
	if err != nil {
		slog.Warn("failed to sign custom referral cookie", "error", err)
		return
	}
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     service.CustomReferralCookieName,
		Value:    encodeCookieValue(signed),
		Path:     "/",
		MaxAge:   landing.CookieTTLDays * 24 * 60 * 60,
		HttpOnly: true,
		Secure:   isRequestHTTPS(c),
		SameSite: http.SameSiteLaxMode,
	})
}

func (h *ReferralHandler) ServeAsset(c *gin.Context) {
	if h == nil || h.assetService == nil || h.referralService == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	publicPath := "/referral-assets/" + strings.TrimLeft(c.Param("path"), "/")
	if !h.referralService.VerifyReferralAssetURL(publicPath, c.Query("expires"), c.Query("sig"), time.Now()) {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}
	filePath, err := h.assetService.ResolvePublicPath(publicPath)
	if err != nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	c.Header("Cache-Control", "private, max-age=60")
	c.File(filePath)
}

func (h *ReferralHandler) GetSummary(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	dashboard, err := h.referralService.GetDashboard(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, dashboard)
}

func (h *ReferralHandler) GetProfile(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	item, err := h.referralService.GetProfile(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) ApplyAffiliate(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	var req applyReferralRequest
	if err := c.ShouldBindJSON(&req); err != nil && err.Error() != "EOF" {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	item, err := h.referralService.ApplyAffiliate(c.Request.Context(), subject.UserID, strings.TrimSpace(req.ApplicantNote))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) ListCommissions(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	page, pageSize := response.ParsePagination(c)
	items, total, err := h.referralService.ListUserCommissions(c.Request.Context(), subject.UserID, service.CustomReferralCommissionListParams{
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

func (h *ReferralHandler) CreateWithdrawal(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	var req createReferralWithdrawalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	}
	if idempotencyKey == "" {
		response.ErrorFrom(c, service.ErrCustomReferralInvalidIdempotency)
		return
	}
	item, err := h.referralService.CreateWithdrawal(c.Request.Context(), service.CustomReferralWithdrawalCreateInput{
		UserID:         subject.UserID,
		Amount:         req.Amount,
		AccountType:    strings.TrimSpace(req.AccountType),
		AccountName:    strings.TrimSpace(req.AccountName),
		AccountNo:      strings.TrimSpace(req.AccountNo),
		AccountNetwork: strings.TrimSpace(req.AccountNetwork),
		QRImageURL:     strings.TrimSpace(req.QRImageURL),
		ContactInfo:    strings.TrimSpace(req.ContactInfo),
		ApplicantNote:  strings.TrimSpace(req.ApplicantNote),
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Created(c, item)
}

func (h *ReferralHandler) ListWithdrawals(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	page, pageSize := response.ParsePagination(c)
	items, total, err := h.referralService.ListUserWithdrawals(c.Request.Context(), subject.UserID, service.CustomReferralWithdrawalListParams{
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

func (h *ReferralHandler) CancelWithdrawal(c *gin.Context) {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "User not authenticated")
		return
	}
	withdrawalID, err := strconv.ParseInt(strings.TrimSpace(c.Param("id")), 10, 64)
	if err != nil || withdrawalID <= 0 {
		response.BadRequest(c, "invalid id")
		return
	}
	item, err := h.referralService.CancelWithdrawal(c.Request.Context(), withdrawalID, subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

func (h *ReferralHandler) UploadAsset(c *gin.Context) {
	if h == nil || h.assetService == nil || h.referralService == nil {
		response.InternalError(c, "asset service unavailable")
		return
	}
	maxBytes := h.assetService.MaxImageBytes()
	fileHeader, err := c.FormFile("file")
	if err != nil {
		response.BadRequest(c, "请选择要上传的二维码图片")
		return
	}
	if fileHeader.Size > maxBytes {
		response.BadRequest(c, fmt.Sprintf("上传图片不能超过 %.1f MB", float64(maxBytes)/(1024*1024)))
		return
	}
	file, err := fileHeader.Open()
	if err != nil {
		response.InternalError(c, "无法读取上传文件")
		return
	}
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		response.InternalError(c, "读取上传文件失败")
		return
	}
	if int64(len(data)) > maxBytes {
		response.BadRequest(c, fmt.Sprintf("上传图片不能超过 %.1f MB", float64(maxBytes)/(1024*1024)))
		return
	}
	contentType := http.DetectContentType(data)
	url, err := h.assetService.SaveImage("withdrawal-qr", data, contentType)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	url, err = h.referralService.SignReferralAssetURL(url, time.Now())
	if err != nil {
		response.InternalError(c, "asset signing unavailable")
		return
	}
	response.Success(c, gin.H{"url": url})
}

func referralRegisterRedirect(basePath, code string) string {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		basePath = "/register"
	}
	u, err := url.Parse(basePath)
	if err != nil {
		return "/register"
	}
	q := u.Query()
	q.Set("aff_code", strings.ToUpper(strings.TrimSpace(code)))
	u.RawQuery = q.Encode()
	return u.String()
}

func referralRegisterErrorRedirect() string {
	u := url.URL{Path: "/register"}
	q := u.Query()
	q.Set("referral_error", "invalid_code")
	u.RawQuery = q.Encode()
	return u.String()
}
