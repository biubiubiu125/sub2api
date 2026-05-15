//go:build embed

package web

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func marshalSiteTitleSettings(t *testing.T, siteName string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"site_name": siteName})
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}
	return payload
}

func TestInjectSiteTitle_ReplacesPlainSiteName(t *testing.T) {
	baseHTML := []byte("<html><head><title>Sub2API - AI API Gateway</title></head><body></body></html>")

	got := injectSiteTitle(baseHTML, marshalSiteTitleSettings(t, "My Site"))

	if !bytes.Contains(got, []byte("<title>My Site - AI API Gateway</title>")) {
		t.Fatalf("title not replaced correctly: %s", got)
	}
}

func TestInjectSiteTitle_EscapesMetaInjectionPayload(t *testing.T) {
	baseHTML := []byte("<html><head><title>Sub2API - AI API Gateway</title></head><body></body></html>")
	siteName := `<meta name="robots" content="noindex">`

	got := injectSiteTitle(baseHTML, marshalSiteTitleSettings(t, siteName))
	gotText := string(got)

	if strings.Contains(gotText, `<meta name="robots" content="noindex">`) {
		t.Fatalf("meta tag injection should be escaped: %s", gotText)
	}
	if !strings.Contains(gotText, `&lt;meta name=&#34;robots&#34; content=&#34;noindex&#34;&gt; - AI API Gateway`) {
		t.Fatalf("escaped meta payload missing from title: %s", gotText)
	}
}

func TestInjectSiteTitle_EscapesScriptBreakoutPayload(t *testing.T) {
	baseHTML := []byte("<html><head><title>Sub2API - AI API Gateway</title></head><body></body></html>")
	siteName := `</title><script>alert(1)</script>`

	got := injectSiteTitle(baseHTML, marshalSiteTitleSettings(t, siteName))
	gotText := string(got)

	if strings.Contains(gotText, "<script>alert(1)</script>") {
		t.Fatalf("script tag injection should be escaped: %s", gotText)
	}
	if strings.Count(gotText, "<title>") != 1 || strings.Count(gotText, "</title>") != 1 {
		t.Fatalf("title should remain intact: %s", gotText)
	}
}

func TestInjectSiteTitle_TrimsLongSiteName(t *testing.T) {
	baseHTML := []byte("<html><head><title>Sub2API - AI API Gateway</title></head><body></body></html>")
	longName := strings.Repeat("A", 120)

	got := injectSiteTitle(baseHTML, marshalSiteTitleSettings(t, longName))
	gotText := string(got)

	if !strings.Contains(gotText, strings.Repeat("A", 80)+" - AI API Gateway") {
		t.Fatalf("long title should be truncated to 80 runes: %s", gotText)
	}
	if strings.Contains(gotText, strings.Repeat("A", 81)) {
		t.Fatalf("title should not contain more than 80 consecutive As: %s", gotText)
	}
}
