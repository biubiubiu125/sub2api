package handler

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

type ReferralHandler struct {
	referralService *service.CustomReferralService
	assetService    *service.ReferralAssetService
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
}

func NewReferralHandler(referralService *service.CustomReferralService, assetService *service.ReferralAssetService) *ReferralHandler {
	return &ReferralHandler{
		referralService: referralService,
		assetService:    assetService,
	}
}

func (h *ReferralHandler) CaptureReferral(c *gin.Context) {
	if h == nil || h.referralService == nil {
		c.Redirect(http.StatusFound, "/register")
		return
	}
	landing, err := h.referralService.HandleLanding(
		c.Request.Context(),
		c.Param("code"),
		c.GetHeader("Referer"),
		c.Request.URL.Path,
	)
	if err == nil && landing != nil {
		raw, signErr := h.referralService.BuildSignedCookieValue(landing.Code, time.Now())
		if signErr == nil {
			http.SetCookie(c.Writer, &http.Cookie{
				Name:     service.CustomReferralCookieName,
				Value:    encodeCookieValue(raw),
				Path:     "/",
				MaxAge:   landing.CookieTTLDays * 24 * 60 * 60,
				HttpOnly: true,
				Secure:   isRequestHTTPS(c),
				SameSite: http.SameSiteLaxMode,
			})
			c.Redirect(http.StatusFound, landing.RedirectPath)
			return
		}
	}
	c.Redirect(http.StatusFound, "/register")
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
	if h == nil || h.assetService == nil {
		response.InternalError(c, "asset service unavailable")
		return
	}
	fileHeader, err := c.FormFile("file")
	if err != nil {
		response.BadRequest(c, "请选择要上传的二维码图片")
		return
	}
	file, err := fileHeader.Open()
	if err != nil {
		response.InternalError(c, "无法读取上传文件")
		return
	}
	defer func() { _ = file.Close() }()

	data, err := io.ReadAll(file)
	if err != nil {
		response.InternalError(c, "读取上传文件失败")
		return
	}
	contentType := http.DetectContentType(data)
	url, err := h.assetService.SaveImage("withdrawal-qr", data, contentType)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"url": url})
}
