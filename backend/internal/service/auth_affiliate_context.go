package service

import (
	"context"
	"strings"
)

type affiliateCodeContextKey struct{}
type affiliateSourceContextKey struct{}

const (
	AffiliateBindingSourceCookie = "cookie"
	AffiliateBindingSourceCode   = "code"
)

func ContextWithAffiliateCode(ctx context.Context, affiliateCode string) context.Context {
	code := strings.TrimSpace(affiliateCode)
	if ctx == nil || code == "" {
		return ctx
	}
	return context.WithValue(ctx, affiliateCodeContextKey{}, code)
}

func ContextWithAffiliateSource(ctx context.Context, affiliateSource string) context.Context {
	source := strings.TrimSpace(affiliateSource)
	if ctx == nil || source == "" {
		return ctx
	}
	return context.WithValue(ctx, affiliateSourceContextKey{}, source)
}

func ContextWithAffiliateAttribution(ctx context.Context, affiliateCode, affiliateSource string) context.Context {
	return ContextWithAffiliateSource(ContextWithAffiliateCode(ctx, affiliateCode), affiliateSource)
}

func affiliateCodeFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	code, _ := ctx.Value(affiliateCodeContextKey{}).(string)
	return strings.TrimSpace(code)
}

func affiliateSourceFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	source, _ := ctx.Value(affiliateSourceContextKey{}).(string)
	return strings.TrimSpace(source)
}
