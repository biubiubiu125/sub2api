package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

func TestResolveAffiliateAttribution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	referralSvc := service.NewCustomReferralService(nil, referralHandlerSettingRepo{}, &config.Config{
		JWT: config.JWTConfig{Secret: "test-secret"},
	})
	h := &AuthHandler{customReferralService: referralSvc}

	cookieValue, err := referralSvc.BuildSignedCookieValue("ABC123", time.Now())
	if err != nil {
		t.Fatalf("BuildSignedCookieValue error = %v", err)
	}
	encodedCookie := encodeCookieValue(cookieValue)

	tests := []struct {
		name       string
		rawCode    string
		cookieCode string
		wantCode   string
		wantSource string
	}{
		{name: "cookie only", cookieCode: encodedCookie, wantCode: "ABC123", wantSource: service.AffiliateBindingSourceCookie},
		{name: "explicit only", rawCode: "abc123", wantCode: "ABC123", wantSource: service.AffiliateBindingSourceCode},
		{name: "explicit matches cookie", rawCode: "ABC123", cookieCode: encodedCookie, wantCode: "ABC123", wantSource: service.AffiliateBindingSourceCookie},
		{name: "explicit overrides different cookie", rawCode: "XYZ999", cookieCode: encodedCookie, wantCode: "XYZ999", wantSource: service.AffiliateBindingSourceCode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", nil)
			if tt.cookieCode != "" {
				req.AddCookie(&http.Cookie{Name: service.CustomReferralCookieName, Value: tt.cookieCode})
			}
			c.Request = req

			gotCode, gotSource := h.resolveAffiliateAttribution(c, tt.rawCode)
			if gotCode != tt.wantCode || gotSource != tt.wantSource {
				t.Fatalf("resolveAffiliateAttribution() = (%q, %q), want (%q, %q)", gotCode, gotSource, tt.wantCode, tt.wantSource)
			}
		})
	}
}
