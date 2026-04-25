const AFFILIATE_COOKIE_NAME = 'sub2api_aff_code'

function isBrowser(): boolean {
  return typeof document !== 'undefined'
}

function firstQueryValue(value: unknown): string {
  if (typeof value === 'string') {
    return value
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      if (typeof item === 'string' && item.trim()) {
        return item
      }
    }
  }
  return ''
}

export function normalizeAffiliateCode(raw: string | null | undefined): string {
  return (raw || '').trim().toUpperCase()
}

export function setAffiliateReferralCode(raw: string | null | undefined): string | null {
  if (!isBrowser()) {
    return null
  }

  const code = normalizeAffiliateCode(raw)
  if (!code) {
    return null
  }

  const secure = window.location.protocol === 'https:' ? '; Secure' : ''
  document.cookie =
    `${AFFILIATE_COOKIE_NAME}=${encodeURIComponent(code)}; Path=/; SameSite=Lax${secure}`
  return code
}

export function getAffiliateReferralCode(): string | null {
  if (!isBrowser()) {
    return null
  }

  const prefix = `${AFFILIATE_COOKIE_NAME}=`
  const segments = document.cookie.split(';')
  for (const segment of segments) {
    const trimmed = segment.trim()
    if (!trimmed.startsWith(prefix)) {
      continue
    }
    const value = decodeURIComponent(trimmed.slice(prefix.length))
    const normalized = normalizeAffiliateCode(value)
    return normalized || null
  }
  return null
}

export function clearAffiliateReferralCookie(): void {
  if (!isBrowser()) {
    return
  }

  const secure = window.location.protocol === 'https:' ? '; Secure' : ''
  document.cookie =
    `${AFFILIATE_COOKIE_NAME}=; Path=/; Max-Age=0; SameSite=Lax${secure}`
}

export function captureAffiliateCodeFromQuery(query: Record<string, unknown>): string | null {
  const code = firstQueryValue(query.aff) || firstQueryValue(query.aff_code)
  return setAffiliateReferralCode(code)
}
