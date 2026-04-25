package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type ReferralAssetService struct{}

func NewReferralAssetService() *ReferralAssetService {
	return &ReferralAssetService{}
}

func (s *ReferralAssetService) SaveImage(kind string, data []byte, contentType string) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty upload data")
	}
	ext, err := referralAssetExtension(contentType)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(referralAssetDataDir(), "public", "referral-assets", sanitizeReferralAssetKind(kind))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create referral asset dir: %w", err)
	}
	name, err := randomReferralAssetName()
	if err != nil {
		return "", err
	}
	fileName := name + ext
	filePath := filepath.Join(dir, fileName)
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return "", fmt.Errorf("write referral asset: %w", err)
	}
	return "/referral-assets/" + sanitizeReferralAssetKind(kind) + "/" + fileName, nil
}

func referralAssetExtension(contentType string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(contentType)) {
	case "image/png":
		return ".png", nil
	case "image/jpeg":
		return ".jpg", nil
	case "image/webp":
		return ".webp", nil
	case "image/gif":
		return ".gif", nil
	default:
		return "", fmt.Errorf("unsupported image content type: %s", contentType)
	}
}

func sanitizeReferralAssetKind(kind string) string {
	switch strings.TrimSpace(kind) {
	case "payment-proof":
		return "payment-proof"
	default:
		return "withdrawal-qr"
	}
}

func randomReferralAssetName() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate referral asset name: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func referralAssetDataDir() string {
	if dir := strings.TrimSpace(os.Getenv("DATA_DIR")); dir != "" {
		return dir
	}
	const dockerDataDir = "/app/data"
	if info, err := os.Stat(dockerDataDir); err == nil && info.IsDir() {
		testFile := filepath.Join(dockerDataDir, ".write_test")
		if file, err := os.Create(testFile); err == nil {
			_ = file.Close()
			_ = os.Remove(testFile)
			return dockerDataDir
		}
	}
	return "."
}
