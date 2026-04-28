package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Keep referral image uploads bounded because the handlers read the image into memory before validation.
const defaultReferralAssetMaxUploadBytes int64 = 5 * 1024 * 1024

type ReferralAssetService struct{}

func NewReferralAssetService() *ReferralAssetService {
	return &ReferralAssetService{}
}

func (s *ReferralAssetService) MaxImageBytes() int64 {
	return referralAssetMaxUploadBytes()
}

func (s *ReferralAssetService) SaveImage(kind string, data []byte, contentType string) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty upload data")
	}
	maxBytes := referralAssetMaxUploadBytes()
	if int64(len(data)) > maxBytes {
		return "", fmt.Errorf("image exceeds maximum size of %d bytes", maxBytes)
	}
	ext, err := referralAssetExtension(data, contentType)
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

func (s *ReferralAssetService) ResolvePublicPath(publicPath string) (string, error) {
	publicPath = NormalizeReferralAssetURL(publicPath)
	if !IsReferralAssetPublicPath(publicPath) {
		return "", fmt.Errorf("invalid referral asset path")
	}
	trimmed := strings.TrimPrefix(publicPath, "/referral-assets/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid referral asset path")
	}
	kind := sanitizeReferralAssetKind(parts[0])
	if kind != parts[0] {
		return "", fmt.Errorf("invalid referral asset kind")
	}
	fileName := filepath.Base(parts[1])
	if fileName == "." || fileName == string(filepath.Separator) || fileName != parts[1] {
		return "", fmt.Errorf("invalid referral asset filename")
	}
	return filepath.Join(referralAssetDataDir(), "public", "referral-assets", kind, fileName), nil
}

func NormalizeReferralAssetURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if u, err := url.Parse(raw); err == nil {
		if u.Path != "" {
			raw = u.Path
		}
	}
	raw = strings.ReplaceAll(raw, "\\", "/")
	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}
	return raw
}

func IsReferralAssetPublicPath(raw string) bool {
	raw = NormalizeReferralAssetURL(raw)
	if !strings.HasPrefix(raw, "/referral-assets/") {
		return false
	}
	trimmed := strings.TrimPrefix(raw, "/referral-assets/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return false
	}
	switch parts[0] {
	case "withdrawal-qr", "payment-proof":
	default:
		return false
	}
	fileName := filepath.Base(parts[1])
	if fileName == "." || fileName == string(filepath.Separator) || fileName != parts[1] {
		return false
	}
	switch strings.ToLower(filepath.Ext(fileName)) {
	case ".png", ".jpg", ".jpeg", ".webp", ".gif":
		return true
	default:
		return false
	}
}

func referralAssetExtension(data []byte, contentType string) (string, error) {
	declared := normalizeReferralAssetContentType(contentType)
	detected := detectReferralAssetContentType(data)
	if detected == "" {
		return "", fmt.Errorf("unsupported image content type: %s", contentType)
	}
	if declared != "" && declared != detected {
		return "", fmt.Errorf("image content type mismatch: declared %s, detected %s", declared, detected)
	}
	switch detected {
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

func normalizeReferralAssetContentType(contentType string) string {
	base := strings.ToLower(strings.TrimSpace(contentType))
	if idx := strings.Index(base, ";"); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}
	return base
}

func detectReferralAssetContentType(data []byte) string {
	if len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
		return "image/webp"
	}
	switch http.DetectContentType(data) {
	case "image/png":
		return "image/png"
	case "image/jpeg":
		return "image/jpeg"
	case "image/gif":
		return "image/gif"
	default:
		return ""
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

func referralAssetMaxUploadBytes() int64 {
	raw := strings.TrimSpace(os.Getenv("REFERRAL_ASSET_MAX_UPLOAD_BYTES"))
	if raw == "" {
		return defaultReferralAssetMaxUploadBytes
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return defaultReferralAssetMaxUploadBytes
	}
	return value
}
