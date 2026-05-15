//go:build embed

package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type stubPublicSettingsProvider struct{}

func (stubPublicSettingsProvider) GetPublicSettingsForInjection(context.Context) (any, error) {
	return map[string]any{
		"site_name": "Sub2API Test",
	}, nil
}

func newFrontendTestRouter(t *testing.T) *gin.Engine {
	t.Helper()

	gin.SetMode(gin.TestMode)

	frontendServer, err := NewFrontendServer(stubPublicSettingsProvider{})
	require.NoError(t, err)

	r := gin.New()
	r.Use(frontendServer.Middleware())

	r.GET("/api/v1/settings/public", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"code":    0,
			"message": "success",
			"data": gin.H{
				"site_name": "Sub2API Test",
			},
		})
	})

	r.GET("/v1/models", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"data": []gin.H{
				{"id": "test-model"},
			},
		})
	})

	r.POST("/chat/completions", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.POST("/responses", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.POST("/backend-api/codex/responses", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.POST("/images/generations", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.GET("/antigravity/models", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"data": []string{"test-model"}})
	})

	r.GET("/setup/status", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"needs_setup": false,
			},
		})
	})

	return r
}

func TestEmbeddedFrontendServesSPAHTMLRoutes(t *testing.T) {
	router := newFrontendTestRouter(t)

	for _, path := range []string{"/", "/home", "/login", "/admin/dashboard", "/setup", "/chat"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			require.Contains(t, rec.Header().Get("Content-Type"), "text/html")
			require.Contains(t, strings.ToLower(rec.Body.String()), "<!doctype html>")
		})
	}
}

func TestEmbeddedFrontendBypassesAPIRoutes(t *testing.T) {
	router := newFrontendTestRouter(t)

	t.Run("/api/v1/settings/public returns JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/settings/public", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Header().Get("Content-Type"), "application/json")

		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.EqualValues(t, 0, body["code"])
	})

	t.Run("/v1/models does not return HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.NotContains(t, strings.ToLower(rec.Header().Get("Content-Type")), "text/html")
		require.NotContains(t, strings.ToLower(rec.Body.String()), "<!doctype html>")
	})

	t.Run("/chat/completions does not return HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/chat/completions", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.NotContains(t, strings.ToLower(rec.Header().Get("Content-Type")), "text/html")
	})

	t.Run("/responses does not return HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/responses", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.NotContains(t, strings.ToLower(rec.Header().Get("Content-Type")), "text/html")
	})

	t.Run("/backend-api/codex/responses does not return HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/backend-api/codex/responses", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.NotContains(t, strings.ToLower(rec.Header().Get("Content-Type")), "text/html")
	})

	t.Run("/images/generations does not return HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/images/generations", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.NotContains(t, strings.ToLower(rec.Header().Get("Content-Type")), "text/html")
	})

	t.Run("/antigravity/models does not return HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/antigravity/models", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.NotContains(t, strings.ToLower(rec.Header().Get("Content-Type")), "text/html")
	})
}
