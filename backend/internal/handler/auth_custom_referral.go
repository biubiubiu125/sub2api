package handler

import (
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

func (h *AuthHandler) SetCustomReferralService(customReferralService *service.CustomReferralService) {
	if h == nil {
		return
	}
	h.customReferralService = customReferralService
}

func (h *AuthHandler) resolveAffiliateCode(c *gin.Context, raw string) string {
	code := strings.ToUpper(strings.TrimSpace(raw))
	if code != "" {
		return code
	}
	if h == nil || h.customReferralService == nil || c == nil {
		return ""
	}
	cookie, err := c.Request.Cookie(service.CustomReferralCookieName)
	if err != nil {
		return ""
	}
	decoded, err := decodeCookieValue(cookie.Value)
	if err != nil {
		return ""
	}
	code, err = h.customReferralService.ParseSignedCookieValue(decoded)
	if err != nil {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(code))
}

func (h *AuthHandler) clearCustomReferralCookie(c *gin.Context) {
	if c == nil {
		return
	}
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     service.CustomReferralCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   isRequestHTTPS(c),
		SameSite: http.SameSiteLaxMode,
	})
}
