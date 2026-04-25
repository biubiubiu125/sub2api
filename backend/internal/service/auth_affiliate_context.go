package service

import (
	"context"
	"strings"
)

type affiliateCodeContextKey struct{}

func ContextWithAffiliateCode(ctx context.Context, affiliateCode string) context.Context {
	code := strings.TrimSpace(affiliateCode)
	if ctx == nil || code == "" {
		return ctx
	}
	return context.WithValue(ctx, affiliateCodeContextKey{}, code)
}

func affiliateCodeFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	code, _ := ctx.Value(affiliateCodeContextKey{}).(string)
	return strings.TrimSpace(code)
}
