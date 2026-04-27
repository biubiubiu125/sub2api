package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

type referralHandlerSettingRepo struct{}

func (referralHandlerSettingRepo) Get(context.Context, string) (*service.Setting, error) {
	return nil, nil
}
func (referralHandlerSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	switch key {
	case service.SettingKeyCustomReferralProvider:
		return service.CustomReferralProviderCustom, nil
	case service.SettingKeyCustomReferralDefaultRate:
		return "5", nil
	default:
		return "", service.ErrSettingNotFound
	}
}
func (referralHandlerSettingRepo) Set(context.Context, string, string) error { return nil }
func (referralHandlerSettingRepo) GetMultiple(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}
func (referralHandlerSettingRepo) SetMultiple(context.Context, map[string]string) error { return nil }
func (referralHandlerSettingRepo) GetAll(context.Context) (map[string]string, error) {
	return map[string]string{}, nil
}
func (referralHandlerSettingRepo) Delete(context.Context, string) error { return nil }

type referralHandlerRepo struct {
	service.CustomReferralRepository
	click service.CustomReferralClickInput
	err   error
}

func (r *referralHandlerRepo) GetApprovedAffiliateByCode(context.Context, string) (*service.CustomAffiliate, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &service.CustomAffiliate{
		ID:                 10,
		UserID:             20,
		InviteCode:         "ABC123",
		AcquisitionEnabled: true,
	}, nil
}

func (r *referralHandlerRepo) RecordReferralClick(_ context.Context, _ int64, _ string, click service.CustomReferralClickInput) error {
	r.click = click
	return nil
}

func TestCaptureReferralRedirectsWithQueryAndDoesNotSetCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &referralHandlerRepo{}
	referralSvc := service.NewCustomReferralService(repo, referralHandlerSettingRepo{}, &config.Config{
		JWT: config.JWTConfig{Secret: "test-secret"},
	})
	h := NewReferralHandler(referralSvc, service.NewReferralAssetService())

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/r/abc123", nil)
	req.Header.Set("Referer", "https://example.com/from")
	req.Header.Set("User-Agent", "browser-a")
	req.RemoteAddr = "203.0.113.8:12345"
	c.Request = req
	c.Params = gin.Params{{Key: "code", Value: "abc123"}}

	h.CaptureReferral(c)

	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusFound)
	}
	if got := w.Header().Get("Location"); got != "/register?aff_code=ABC123" {
		t.Fatalf("Location = %q", got)
	}
	if cookies := w.Header().Values("Set-Cookie"); len(cookies) != 0 {
		t.Fatalf("CaptureReferral set cookies for landing request: %v", cookies)
	}
	if repo.click.IPHash == "" || repo.click.UserAgentHash == "" {
		t.Fatalf("click risk hashes were not recorded: %+v", repo.click)
	}
	if strings.Contains(repo.click.IPHash, "203.0.113.8") || strings.Contains(repo.click.UserAgentHash, "browser-a") {
		t.Fatalf("click risk fields contain raw client data: %+v", repo.click)
	}
}

func TestCaptureReferralRedirectsInvalidCodeWithExplicitError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &referralHandlerRepo{err: service.ErrCustomReferralAffiliateDisabled}
	referralSvc := service.NewCustomReferralService(repo, referralHandlerSettingRepo{}, &config.Config{
		JWT: config.JWTConfig{Secret: "test-secret"},
	})
	h := NewReferralHandler(referralSvc, service.NewReferralAssetService())

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/r/bad-code", nil)
	c.Request = req
	c.Params = gin.Params{{Key: "code", Value: "bad-code"}}

	h.CaptureReferral(c)

	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusFound)
	}
	if got := w.Header().Get("Location"); got != "/register?referral_error=invalid_code" {
		t.Fatalf("Location = %q", got)
	}
	if cookies := w.Header().Values("Set-Cookie"); len(cookies) != 0 {
		t.Fatalf("invalid referral set cookies: %v", cookies)
	}
}

func TestServeReferralAssetRequiresSignedURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	assetDir := filepath.Join(dataDir, "public", "referral-assets", "withdrawal-qr")
	if err := os.MkdirAll(assetDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(assetDir, "qr.png"), []byte("png"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	referralSvc := service.NewCustomReferralService(nil, nil, &config.Config{
		JWT: config.JWTConfig{Secret: "asset-secret"},
	})
	h := NewReferralHandler(referralSvc, service.NewReferralAssetService())
	r := gin.New()
	r.GET("/referral-assets/*path", h.ServeAsset)

	unsigned := httptest.NewRecorder()
	r.ServeHTTP(unsigned, httptest.NewRequest(http.MethodGet, "/referral-assets/withdrawal-qr/qr.png", nil))
	if unsigned.Code != http.StatusForbidden {
		t.Fatalf("unsigned asset status = %d, want %d", unsigned.Code, http.StatusForbidden)
	}

	signed, err := referralSvc.SignReferralAssetURL("/referral-assets/withdrawal-qr/qr.png", time.Now())
	if err != nil {
		t.Fatalf("SignReferralAssetURL returned error: %v", err)
	}
	signedResp := httptest.NewRecorder()
	r.ServeHTTP(signedResp, httptest.NewRequest(http.MethodGet, signed, nil))
	if signedResp.Code != http.StatusOK {
		t.Fatalf("signed asset status = %d, want %d", signedResp.Code, http.StatusOK)
	}
}
