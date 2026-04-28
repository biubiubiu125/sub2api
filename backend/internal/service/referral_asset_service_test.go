package service

import (
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func TestReferralAssetExtensionRejectsDeclaredTypeMismatch(t *testing.T) {
	png := append([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}, make([]byte, 32)...)
	if _, err := referralAssetExtension(png, "image/jpeg"); err == nil {
		t.Fatalf("referralAssetExtension accepted mismatched declared type")
	}
}

func TestReferralAssetExtensionRejectsFakeImagePayload(t *testing.T) {
	fake := []byte("<script>alert(1)</script>")
	if _, err := referralAssetExtension(fake, "image/png"); err == nil {
		t.Fatalf("referralAssetExtension accepted non-image payload")
	}
}

func TestReferralAssetExtensionAcceptsWebPByMagicBytes(t *testing.T) {
	webp := append([]byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P'}, make([]byte, 32)...)
	ext, err := referralAssetExtension(webp, "image/webp")
	if err != nil {
		t.Fatalf("referralAssetExtension returned error: %v", err)
	}
	if ext != ".webp" {
		t.Fatalf("extension = %s, want .webp", ext)
	}
}

func TestReferralAssetURLSigningRequiresValidSignature(t *testing.T) {
	svc := NewCustomReferralService(nil, nil, &config.Config{
		JWT: config.JWTConfig{Secret: "asset-secret"},
	})
	now := time.Unix(1000, 0)
	signed, err := svc.SignReferralAssetURL("/referral-assets/withdrawal-qr/abc123.png", now)
	if err != nil {
		t.Fatalf("SignReferralAssetURL returned error: %v", err)
	}
	if !strings.Contains(signed, "expires=") || !strings.Contains(signed, "sig=") {
		t.Fatalf("signed url missing signature fields: %s", signed)
	}
	if !svc.VerifyReferralAssetURL("/referral-assets/withdrawal-qr/abc123.png", "1900", signed[strings.LastIndex(signed, "sig=")+4:], now) {
		t.Fatalf("VerifyReferralAssetURL rejected a valid signature")
	}
	if svc.VerifyReferralAssetURL("/referral-assets/withdrawal-qr/abc123.png", "1900", "bad-signature", now) {
		t.Fatalf("VerifyReferralAssetURL accepted an invalid signature")
	}
	if svc.VerifyReferralAssetURL("/referral-assets/withdrawal-qr/abc123.png", "900", signed[strings.LastIndex(signed, "sig=")+4:], now) {
		t.Fatalf("VerifyReferralAssetURL accepted an expired signature")
	}
}

func TestNormalizeReferralAssetURLStripsQueryBeforePersistence(t *testing.T) {
	raw := "/referral-assets/payment-proof/proof.webp?expires=123&sig=abc"
	if got := NormalizeReferralAssetURL(raw); got != "/referral-assets/payment-proof/proof.webp" {
		t.Fatalf("NormalizeReferralAssetURL = %q", got)
	}
	if IsReferralAssetPublicPath("/referral-assets/payment-proof/../../secret.png") {
		t.Fatalf("IsReferralAssetPublicPath accepted traversal")
	}
}
