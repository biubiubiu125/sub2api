//go:build embed

package web

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"html"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/setup"
	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

const (
	// NonceHTMLPlaceholder is the placeholder for nonce in HTML script tags
	NonceHTMLPlaceholder = "__CSP_NONCE_VALUE__"
	frontendPrivateKey   = "frontend_private_route"
)

//go:embed all:dist
var frontendFS embed.FS

// PublicSettingsProvider is an interface to fetch public settings
type PublicSettingsProvider interface {
	GetPublicSettingsForInjection(ctx context.Context) (any, error)
}

// FrontendServer serves the embedded frontend with settings injection
type FrontendServer struct {
	distFS        fs.FS
	fileServer    http.Handler
	baseHTML      []byte
	cache         *HTMLCache
	settings      PublicSettingsProvider
	overrideDir   string // local file override directory
	pagesDir      string
	htmlUserAuth  gin.HandlerFunc
	htmlAdminAuth gin.HandlerFunc
}

// NewFrontendServer creates a new frontend server with settings injection
func NewFrontendServer(settingsProvider PublicSettingsProvider, authGuards ...gin.HandlerFunc) (*FrontendServer, error) {
	distFS, err := fs.Sub(frontendFS, "dist")
	if err != nil {
		return nil, err
	}

	// Read base HTML once
	file, err := distFS.Open("index.html")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	baseHTML, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	cache := NewHTMLCache()
	cache.SetBaseHTML(baseHTML)

	var userAuth gin.HandlerFunc
	var adminAuth gin.HandlerFunc
	if len(authGuards) > 0 {
		userAuth = authGuards[0]
	}
	if len(authGuards) > 1 {
		adminAuth = authGuards[1]
	}

	dataDir := frontendDataDir()

	return &FrontendServer{
		distFS:        distFS,
		fileServer:    http.FileServer(http.FS(distFS)),
		baseHTML:      baseHTML,
		cache:         cache,
		settings:      settingsProvider,
		overrideDir:   filepath.Join(dataDir, "public"),
		pagesDir:      filepath.Join(dataDir, "pages"),
		htmlUserAuth:  userAuth,
		htmlAdminAuth: adminAuth,
	}, nil
}

// InvalidateCache invalidates the HTML cache (call when settings change)
func (s *FrontendServer) InvalidateCache() {
	if s != nil && s.cache != nil {
		s.cache.Invalidate()
	}
}

func frontendDataDir() string {
	base := setup.GetDataDir()
	type pricingConfig struct {
		DataDir string `yaml:"data_dir"`
	}
	type appConfig struct {
		Pricing pricingConfig `yaml:"pricing"`
	}

	cfgPath := filepath.Join(base, setup.ConfigFileName)
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return base
	}
	var cfg appConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return base
	}
	if trimmed := strings.TrimSpace(cfg.Pricing.DataDir); trimmed != "" {
		return trimmed
	}
	return base
}

func normalizeRequestPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "/"
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return trimmed
}

// Middleware returns the Gin middleware handler
func (s *FrontendServer) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := normalizeRequestPath(c.Request.URL.Path)

		// Skip API routes
		if shouldBypassEmbeddedFrontend(path) {
			c.Next()
			return
		}

		cleanPath := strings.TrimPrefix(path, "/")
		if cleanPath == "" {
			cleanPath = "index.html"
		}

		if requiresHTMLAuth(path) {
			allowed, status := s.authorizeHTMLRoute(c, path)
			if !allowed {
				if status == http.StatusForbidden {
					c.String(http.StatusForbidden, "Forbidden")
				} else {
					redirect := "/login"
					if path != "" && path != "/" {
						redirect = "/login?redirect=" + url.QueryEscape(path)
					}
					c.Redirect(http.StatusFound, redirect)
				}
				c.Abort()
				return
			}
		}

		if strings.Contains(filepath.Base(cleanPath), ".") && !s.fileExists(cleanPath) {
			c.String(http.StatusNotFound, "Frontend not found")
			c.Abort()
			return
		}

		// For index.html or SPA routes, serve with injected settings
		if cleanPath == "index.html" || !s.fileExists(cleanPath) {
			if requiresHTMLAuth(path) {
				c.Set(frontendPrivateKey, true)
			}
			s.serveIndexHTML(c)
			return
		}

		// Try local override first
		if s.tryServeOverride(c, cleanPath) {
			return
		}

		// Serve static files normally
		s.fileServer.ServeHTTP(c.Writer, c.Request)
		c.Abort()
	}
}

func requiresHTMLAuth(path string) bool {
	switch {
	case strings.HasPrefix(path, "/admin"):
		return true
	case path == "/dashboard":
		return true
	case hasKnownRoutePrefix(path,
		"/keys",
		"/usage",
		"/redeem",
		"/affiliate",
		"/available-channels",
		"/profile",
		"/subscriptions",
		"/purchase",
		"/orders",
		"/payment",
		"/monitor",
	):
		return true
	default:
		return false
	}
}

func (s *FrontendServer) authorizeHTMLRoute(c *gin.Context, path string) (bool, int) {
	if c == nil || c.Request == nil {
		return false, http.StatusUnauthorized
	}
	var guard gin.HandlerFunc
	if strings.HasPrefix(path, "/admin") {
		guard = s.htmlAdminAuth
	} else {
		guard = s.htmlUserAuth
	}
	if guard == nil {
		return true, http.StatusOK
	}
	guard(c)
	if c.IsAborted() {
		return false, c.Writer.Status()
	}
	return true, http.StatusOK
}

func (s *FrontendServer) fileExists(path string) bool {
	file, err := s.distFS.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// tryServeOverride checks if a local override file exists and serves it.
// Files in overrideDir take precedence over embedded files.
func (s *FrontendServer) tryServeOverride(c *gin.Context, cleanPath string) bool {
	if s.overrideDir == "" {
		return false
	}
	filePath := filepath.Join(s.overrideDir, filepath.Clean("/"+cleanPath))
	info, err := os.Stat(filePath)
	if err != nil || info.IsDir() {
		return false
	}
	c.File(filePath)
	c.Abort()
	return true
}

func (s *FrontendServer) serveIndexHTML(c *gin.Context) {
	// Get nonce from context (generated by SecurityHeaders middleware)
	nonce := middleware.GetNonceFromContext(c)

	// Check cache first
	cached := s.cache.Get()
	if cached != nil {
		// Check If-None-Match for 304 response
		if match := c.GetHeader("If-None-Match"); match == cached.ETag {
			c.Status(http.StatusNotModified)
			c.Abort()
			return
		}

		// Replace nonce placeholder with actual nonce before serving
		content := replaceNoncePlaceholder(cached.Content, nonce)

		c.Header("ETag", cached.ETag)
		c.Header("Cache-Control", "no-cache") // Must revalidate
		c.Data(http.StatusOK, "text/html; charset=utf-8", content)
		c.Abort()
		return
	}

	// Cache miss - fetch settings and render
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	settings, err := s.settings.GetPublicSettingsForInjection(ctx)
	if err != nil {
		// Fallback: serve without injection
		c.Data(http.StatusOK, "text/html; charset=utf-8", s.baseHTML)
		c.Abort()
		return
	}

	settingsJSON, err := json.Marshal(settings)
	if err != nil {
		// Fallback: serve without injection
		c.Data(http.StatusOK, "text/html; charset=utf-8", s.baseHTML)
		c.Abort()
		return
	}

	rendered := s.injectSettings(settingsJSON)
	s.cache.Set(rendered, settingsJSON)

	// Replace nonce placeholder with actual nonce before serving
	content := replaceNoncePlaceholder(rendered, nonce)

	cached = s.cache.Get()
	if cached != nil {
		c.Header("ETag", cached.ETag)
	}
	c.Header("Cache-Control", "no-cache")
	c.Data(http.StatusOK, "text/html; charset=utf-8", content)
	c.Abort()
}

func hasKnownRoutePrefix(path string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func (s *FrontendServer) injectSettings(settingsJSON []byte) []byte {
	// Create the script tag to inject with nonce placeholder
	// The placeholder will be replaced with actual nonce at request time
	script := []byte(`<script nonce="` + NonceHTMLPlaceholder + `">window.__APP_CONFIG__=` + string(settingsJSON) + `;</script>`)

	// Inject before </head>
	headClose := []byte("</head>")
	result := bytes.Replace(s.baseHTML, headClose, append(script, headClose...), 1)

	// Replace <title> with custom site name so the browser tab shows it immediately
	result = injectSiteTitle(result, settingsJSON)

	return result
}

// injectSiteTitle replaces the static <title> in HTML with the configured site name.
// This ensures the browser tab shows the correct title before JS executes.
func injectSiteTitle(docHTML, settingsJSON []byte) []byte {
	var cfg struct {
		SiteName string `json:"site_name"`
	}
	if err := json.Unmarshal(settingsJSON, &cfg); err != nil {
		return docHTML
	}

	siteName := strings.TrimSpace(cfg.SiteName)
	if siteName == "" {
		return docHTML
	}
	siteName = strings.ReplaceAll(siteName, "\r", " ")
	siteName = strings.ReplaceAll(siteName, "\n", " ")
	siteName = strings.Join(strings.Fields(siteName), " ")
	if siteName == "" {
		return docHTML
	}

	const maxSiteTitleRunes = 80
	siteName = trimRunes(siteName, maxSiteTitleRunes)
	escapedSiteName := html.EscapeString(siteName)
	if escapedSiteName == "" {
		return docHTML
	}

	// Find and replace the existing <title>...</title>
	titleStart := bytes.Index(docHTML, []byte("<title>"))
	titleEnd := bytes.Index(docHTML, []byte("</title>"))
	if titleStart == -1 || titleEnd == -1 || titleEnd <= titleStart {
		return docHTML
	}

	newTitle := []byte("<title>" + escapedSiteName + " - AI API Gateway</title>")
	var buf bytes.Buffer
	buf.Write(docHTML[:titleStart])
	buf.Write(newTitle)
	buf.Write(docHTML[titleEnd+len("</title>"):])
	return buf.Bytes()
}

func trimRunes(value string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes])
}

// replaceNoncePlaceholder replaces the nonce placeholder with actual nonce value
func replaceNoncePlaceholder(html []byte, nonce string) []byte {
	return bytes.ReplaceAll(html, []byte(NonceHTMLPlaceholder), []byte(nonce))
}

// ServeEmbeddedFrontend returns a middleware for serving embedded frontend
// This is the legacy function for backward compatibility when no settings provider is available
func ServeEmbeddedFrontend() gin.HandlerFunc {
	distFS, err := fs.Sub(frontendFS, "dist")
	if err != nil {
		panic("failed to get dist subdirectory: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(distFS))
	overrideDir := filepath.Join("data", "public")

	return func(c *gin.Context) {
		path := c.Request.URL.Path

		if shouldBypassEmbeddedFrontend(path) {
			c.Next()
			return
		}

		cleanPath := strings.TrimPrefix(path, "/")
		if cleanPath == "" {
			cleanPath = "index.html"
		}

		if file, err := distFS.Open(cleanPath); err == nil {
			_ = file.Close()
			// Try local override first
			if tryServeOverrideFile(c, overrideDir, cleanPath) {
				return
			}
			fileServer.ServeHTTP(c.Writer, c.Request)
			c.Abort()
			return
		}

		serveIndexHTML(c, distFS)
	}
}

// tryServeOverrideFile is a standalone version of tryServeOverride for legacy usage.
func tryServeOverrideFile(c *gin.Context, overrideDir, cleanPath string) bool {
	if overrideDir == "" {
		return false
	}
	filePath := filepath.Join(overrideDir, filepath.Clean("/"+cleanPath))
	info, err := os.Stat(filePath)
	if err != nil || info.IsDir() {
		return false
	}
	c.File(filePath)
	c.Abort()
	return true
}

func shouldBypassEmbeddedFrontend(path string) bool {
	trimmed := strings.TrimSpace(path)
	return strings.HasPrefix(trimmed, "/api/") ||
		strings.HasPrefix(trimmed, "/v1/") ||
		strings.HasPrefix(trimmed, "/v1beta/") ||
		strings.HasPrefix(trimmed, "/backend-api/") ||
		strings.HasPrefix(trimmed, "/antigravity/") ||
		strings.HasPrefix(trimmed, "/setup/") ||
		trimmed == "/health" ||
		trimmed == "/responses" ||
		strings.HasPrefix(trimmed, "/responses/") ||
		strings.HasPrefix(trimmed, "/images/")
}

func serveIndexHTML(c *gin.Context, fsys fs.FS) {
	file, err := fsys.Open("index.html")
	if err != nil {
		c.String(http.StatusNotFound, "Frontend not found")
		c.Abort()
		return
	}
	defer func() { _ = file.Close() }()

	content, err := io.ReadAll(file)
	if err != nil {
		c.String(http.StatusInternalServerError, "Failed to read index.html")
		c.Abort()
		return
	}

	c.Data(http.StatusOK, "text/html; charset=utf-8", content)
	c.Abort()
}

func HasEmbeddedFrontend() bool {
	_, err := frontendFS.ReadFile("dist/index.html")
	return err == nil
}
