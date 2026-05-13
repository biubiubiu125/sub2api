import createDOMPurify from 'dompurify'
import { marked } from 'marked'

marked.setOptions({
  breaks: true,
  gfm: true,
})

const PUBLIC_CONTENT_ALLOWED_TAGS = [
  'p', 'br', 'strong', 'b', 'em', 'i', 'u', 's',
  'span', 'a', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
  'ul', 'ol', 'li', 'blockquote', 'pre', 'code', 'hr', 'img', 'div',
  'table', 'thead', 'tbody', 'tr', 'th', 'td',
  'header', 'main', 'section', 'article', 'aside', 'footer', 'nav', 'figure', 'figcaption',
  'svg', 'g', 'path', 'circle', 'rect', 'line', 'polyline', 'polygon', 'ellipse',
] as const

const PUBLIC_CONTENT_ALLOWED_ATTR = [
  'href', 'target', 'rel', 'src', 'alt', 'title', 'style', 'class', 'id',
  'viewBox', 'xmlns', 'width', 'height', 'fill', 'stroke', 'stroke-width',
  'stroke-linecap', 'stroke-linejoin', 'd', 'cx', 'cy', 'r', 'x', 'y',
  'rx', 'ry', 'x1', 'y1', 'x2', 'y2', 'points', 'opacity', 'transform',
  'fill-rule', 'clip-rule', 'role', 'aria-hidden', 'focusable', 'preserveAspectRatio',
] as const
const PUBLIC_CONTENT_SAFE_TEXT_ALIGN = new Set(['left', 'center', 'right', 'justify'])
const PUBLIC_CONTENT_SAFE_COLOR_NAMES = new Set([
  'black', 'white', 'red', 'blue', 'green', 'yellow', 'orange', 'purple',
  'gray', 'grey', 'teal', 'pink', 'brown',
])
const PUBLIC_CONTENT_CLASS_ALLOWED_TAGS = new Set([
  'a', 'img', 'span', 'p', 'div', 'blockquote', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
  'strong', 'b', 'em', 'i', 'u', 's', 'ul', 'ol', 'li', 'pre', 'code', 'hr',
  'table', 'thead', 'tbody', 'tr', 'th', 'td',
  'header', 'main', 'section', 'article', 'aside', 'footer', 'nav', 'figure', 'figcaption',
  'svg', 'g', 'path', 'circle', 'rect', 'line', 'polyline', 'polygon', 'ellipse',
])
const PUBLIC_CONTENT_SVG_TAGS = new Set([
  'svg', 'g', 'path', 'circle', 'rect', 'line', 'polyline', 'polygon', 'ellipse',
])

const rgbColorPattern = /^rgba?\(\s*(\d{1,3}\s*,\s*){2}\d{1,3}(\s*,\s*(0|0?\.\d+|1(\.0+)?)\s*)?\)$/
const hslColorPattern = /^hsla?\(\s*\d{1,3}(\.\d+)?\s*,\s*\d{1,3}%\s*,\s*\d{1,3}%(\s*,\s*(0|0?\.\d+|1(\.0+)?)\s*)?\)$/
const languageClassPattern = /^language-[a-z0-9_-]+$/i
const svgPathDataPattern = /^[a-z0-9 ,.+\-]+$/i
const svgPointsPattern = /^[0-9 ,.+\-]+$/i
const svgTransformPattern = /^(matrix|translate|scale|rotate|skewX|skewY)\([^()]+\)(\s+(matrix|translate|scale|rotate|skewX|skewY)\([^()]+\))*$/i
const svgNumberishPattern = /^-?\d+(\.\d+)?(%|px|em|rem)?$/i
const svgViewBoxPattern = /^-?\d+(\.\d+)?(\s+-?\d+(\.\d+)?){3}$/

type DOMPurifyLike = {
  sanitize: (dirty: string, config?: Record<string, unknown>) => string
}

type PublicContentOptions = {
  pageSlug?: string
}

let browserPurifier: DOMPurifyLike | null = null

function getBrowserPurifier(): DOMPurifyLike {
  if (browserPurifier) {
    return browserPurifier
  }
  if (typeof window === 'undefined') {
    throw new Error('sanitizePublicHTML requires a browser-like window')
  }
  browserPurifier = createDOMPurify(window)
  return browserPurifier
}

function sanitizeStyle(style: string): string {
  const allowed: string[] = []

  for (const part of style.split(';')) {
    const trimmed = part.trim()
    if (!trimmed) {
      continue
    }
    const [rawKey, rawValue] = trimmed.split(':', 2)
    if (!rawKey || !rawValue) {
      continue
    }
    const key = rawKey.trim().toLowerCase()
    const value = rawValue.trim()
    switch (key) {
      case 'color':
      case 'background-color':
        if (isSafeCSSColor(value)) {
          allowed.push(`${key}: ${value}`)
        }
        break
      case 'text-align':
        if (PUBLIC_CONTENT_SAFE_TEXT_ALIGN.has(value.toLowerCase())) {
          allowed.push(`${key}: ${value.toLowerCase()}`)
        }
        break
      default:
        break
    }
  }

  return allowed.join('; ')
}

function isSafeCSSColor(value: string): boolean {
  const normalized = value.trim().toLowerCase()
  if (!normalized) {
    return false
  }
  if (normalized.startsWith('#')) {
    const hex = normalized.slice(1)
    return (hex.length === 3 || hex.length === 6) && /^[0-9a-f]+$/i.test(hex)
  }
  return rgbColorPattern.test(normalized)
    || hslColorPattern.test(normalized)
    || PUBLIC_CONTENT_SAFE_COLOR_NAMES.has(normalized)
}

function isSafeSVGPaint(value: string): boolean {
  const normalized = value.trim()
  if (!normalized) {
    return false
  }
  const lowered = normalized.toLowerCase()
  return lowered === 'none' || lowered === 'currentcolor' || isSafeCSSColor(normalized)
}

function isSafeSVGNumberish(value: string): boolean {
  return svgNumberishPattern.test(value.trim())
}

function isSafeSVGViewBox(value: string): boolean {
  return svgViewBoxPattern.test(value.trim().replace(/\s+/g, ' '))
}

function isSafeSVGPathData(value: string): boolean {
  return svgPathDataPattern.test(value.trim())
}

function isSafeSVGPoints(value: string): boolean {
  return svgPointsPattern.test(value.trim())
}

function isSafeSVGTransform(value: string): boolean {
  return svgTransformPattern.test(value.trim())
}

function isAllowedHref(raw: string): boolean {
  const trimmed = raw.trim()
  if (!trimmed) {
    return false
  }
  if (trimmed.startsWith('#') || (trimmed.startsWith('/') && !trimmed.startsWith('//'))) {
    return true
  }
  try {
    const parsed = new URL(trimmed)
    return ['http:', 'https:', 'mailto:', 'tel:'].includes(parsed.protocol.toLowerCase())
  } catch {
    return false
  }
}

function isAllowedImageSrc(raw: string): boolean {
  const trimmed = raw.trim()
  if (!trimmed) {
    return false
  }
  if (trimmed.startsWith('/') && !trimmed.startsWith('//')) {
    return true
  }
  try {
    const parsed = new URL(trimmed)
    return ['http:', 'https:'].includes(parsed.protocol.toLowerCase())
  } catch {
    return false
  }
}

function unwrapElement(element: Element): void {
  const parent = element.parentNode
  if (!parent) {
    element.remove()
    return
  }
  while (element.firstChild) {
    parent.insertBefore(element.firstChild, element)
  }
  parent.removeChild(element)
}

function keepOnlyAllowedAttrs(element: HTMLElement, allowed: string[]): void {
  for (const { name } of Array.from(element.attributes)) {
    if (!allowed.includes(name.toLowerCase())) {
      element.removeAttribute(name)
    }
  }
}

function hasClassName(element: HTMLElement): boolean {
  return Boolean(element.getAttribute('class')?.trim())
}

function keepSanitizedClass(element: HTMLElement): void {
  if (!PUBLIC_CONTENT_CLASS_ALLOWED_TAGS.has(element.tagName.toLowerCase())) {
    element.removeAttribute('class')
    return
  }
  const className = element.getAttribute('class')?.trim() ?? ''
  if (!className) {
    element.removeAttribute('class')
    return
  }
  element.setAttribute('class', className)
}

function sanitizeSVGElement(element: HTMLElement): void {
  const tag = element.tagName.toLowerCase()
  const sanitized = new Map<string, string>()
  const read = (key: string) => element.getAttribute(key)?.trim() ?? ''
  const setIf = (key: string, predicate: (value: string) => boolean) => {
    const value = read(key)
    if (value && predicate(value)) {
      sanitized.set(key, value)
    }
  }

  if (hasClassName(element)) {
    sanitized.set('class', read('class'))
  }
  setIf('id', (value) => value.length > 0)
  setIf('role', (value) => value === 'img' || value === 'presentation')
  setIf('aria-hidden', (value) => value === 'true' || value === 'false')
  setIf('focusable', (value) => value === 'true' || value === 'false')
  setIf('opacity', isSafeSVGNumberish)
  setIf('transform', isSafeSVGTransform)

  if (tag === 'svg') {
    setIf('viewBox', isSafeSVGViewBox)
    setIf('xmlns', (value) => value === 'http://www.w3.org/2000/svg')
    setIf('width', isSafeSVGNumberish)
    setIf('height', isSafeSVGNumberish)
    setIf('fill', isSafeSVGPaint)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
    setIf('stroke-linecap', (value) => ['round', 'square', 'butt'].includes(value))
    setIf('stroke-linejoin', (value) => ['round', 'bevel', 'miter'].includes(value))
    setIf('preserveAspectRatio', (value) => /^[a-z0-9\s]+$/i.test(value))
  }

  if (tag === 'path') {
    setIf('d', isSafeSVGPathData)
    setIf('fill', isSafeSVGPaint)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
    setIf('stroke-linecap', (value) => ['round', 'square', 'butt'].includes(value))
    setIf('stroke-linejoin', (value) => ['round', 'bevel', 'miter'].includes(value))
    setIf('fill-rule', (value) => ['evenodd', 'nonzero'].includes(value))
    setIf('clip-rule', (value) => ['evenodd', 'nonzero'].includes(value))
  }

  if (tag === 'circle') {
    setIf('cx', isSafeSVGNumberish)
    setIf('cy', isSafeSVGNumberish)
    setIf('r', isSafeSVGNumberish)
    setIf('fill', isSafeSVGPaint)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
  }

  if (tag === 'ellipse') {
    setIf('cx', isSafeSVGNumberish)
    setIf('cy', isSafeSVGNumberish)
    setIf('rx', isSafeSVGNumberish)
    setIf('ry', isSafeSVGNumberish)
    setIf('fill', isSafeSVGPaint)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
  }

  if (tag === 'rect') {
    setIf('x', isSafeSVGNumberish)
    setIf('y', isSafeSVGNumberish)
    setIf('width', isSafeSVGNumberish)
    setIf('height', isSafeSVGNumberish)
    setIf('rx', isSafeSVGNumberish)
    setIf('ry', isSafeSVGNumberish)
    setIf('fill', isSafeSVGPaint)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
  }

  if (tag === 'line') {
    setIf('x1', isSafeSVGNumberish)
    setIf('y1', isSafeSVGNumberish)
    setIf('x2', isSafeSVGNumberish)
    setIf('y2', isSafeSVGNumberish)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
    setIf('stroke-linecap', (value) => ['round', 'square', 'butt'].includes(value))
  }

  if (tag === 'polyline' || tag === 'polygon') {
    setIf('points', isSafeSVGPoints)
    setIf('fill', isSafeSVGPaint)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
    setIf('stroke-linecap', (value) => ['round', 'square', 'butt'].includes(value))
    setIf('stroke-linejoin', (value) => ['round', 'bevel', 'miter'].includes(value))
  }

  if (tag === 'g') {
    setIf('fill', isSafeSVGPaint)
    setIf('stroke', isSafeSVGPaint)
    setIf('stroke-width', isSafeSVGNumberish)
  }

  for (const { name } of Array.from(element.attributes)) {
    element.removeAttribute(name)
  }
  sanitized.forEach((value, key) => {
    element.setAttribute(key, value)
  })
}

function postProcessSanitizedHTML(root: HTMLElement): void {
  const elements = Array.from(root.querySelectorAll<HTMLElement>('*'))
  for (const element of elements) {
    const tag = element.tagName.toLowerCase()
    if (PUBLIC_CONTENT_SVG_TAGS.has(tag)) {
      sanitizeSVGElement(element)
      continue
    }
    switch (tag) {
      case 'a': {
        const href = element.getAttribute('href')?.trim() ?? ''
        if (!isAllowedHref(href)) {
          unwrapElement(element)
          break
        }
        const target = element.getAttribute('target')?.trim().toLowerCase()
        if (target !== '_blank' && target !== '_self') {
          element.removeAttribute('target')
        } else {
          element.setAttribute('target', target)
        }
        element.setAttribute('href', href)
        element.setAttribute('rel', 'noopener noreferrer nofollow')
        keepSanitizedClass(element)
        keepOnlyAllowedAttrs(element, hasClassName(element) ? ['href', 'target', 'rel', 'class'] : ['href', 'target', 'rel'])
        break
      }
      case 'img': {
        const src = element.getAttribute('src')?.trim() ?? ''
        if (!isAllowedImageSrc(src)) {
          element.remove()
          break
        }
        element.setAttribute('src', src)
        keepSanitizedClass(element)
        keepOnlyAllowedAttrs(element, hasClassName(element) ? ['src', 'alt', 'title', 'class'] : ['src', 'alt', 'title'])
        break
      }
      case 'code': {
        const className = element.getAttribute('class')?.trim() ?? ''
        if (!languageClassPattern.test(className)) {
          element.removeAttribute('class')
        } else {
          element.setAttribute('class', className)
        }
        keepOnlyAllowedAttrs(element, ['class'])
        break
      }
      case 'span':
      case 'p':
      case 'div':
      case 'blockquote':
      case 'h1':
      case 'h2':
      case 'h3':
      case 'h4':
      case 'h5':
      case 'h6': {
        const style = element.getAttribute('style')?.trim() ?? ''
        const sanitizedStyle = sanitizeStyle(style)
        keepSanitizedClass(element)
        if (sanitizedStyle) {
          element.setAttribute('style', sanitizedStyle)
        } else {
          element.removeAttribute('style')
        }
        const allowedAttrs = hasClassName(element) ? ['class'] : []
        if (sanitizedStyle) {
          allowedAttrs.push('style')
        }
        keepOnlyAllowedAttrs(element, allowedAttrs)
        break
      }
      default:
        keepSanitizedClass(element)
        keepOnlyAllowedAttrs(element, hasClassName(element) ? ['class'] : [])
        break
    }
  }
}

export function buildPageImageURL(pageSlug: string, src: string): string {
  let trimmed = String(src ?? '').trim()
  try {
    trimmed = decodeURIComponent(trimmed)
  } catch {
    // Keep the original path when it is not valid percent-encoding.
  }
  const match = trimmed.match(/^([^?#]*)([?#].*)?$/)
  const pathPart = match?.[1] ?? trimmed
  const suffix = match?.[2] ?? ''
  const encodedParts = pathPart
    .split('/')
    .filter((part) => part && part !== '.')
    .map((part) => encodeURIComponent(part))
  return `/api/v1/pages/${encodeURIComponent(pageSlug)}/images/${encodedParts.join('/')}${suffix}`
}

function isRelativePageImageSource(src: string): boolean {
  const trimmed = src.trim()
  if (!trimmed || trimmed.startsWith('/') || trimmed.startsWith('//') || trimmed.includes('\\')) {
    return false
  }
  try {
    const parsed = new URL(trimmed)
    return !parsed.protocol
  } catch {
    return !trimmed.split('/').some((part) => part === '..') && !/^[a-z][a-z0-9+.-]*:/i.test(trimmed)
  }
}

function rewriteRelativeHTMLImageSources(raw: string, document: Document, pageSlug?: string): string {
  const slug = String(pageSlug ?? '').trim()
  if (!slug || !raw.trim()) {
    return raw
  }

  const root = document.createElement('div')
  root.innerHTML = raw
  root.querySelectorAll('img[src]').forEach((element) => {
    const src = element.getAttribute('src')?.trim() ?? ''
    if (!isRelativePageImageSource(src)) {
      return
    }
    element.setAttribute('src', buildPageImageURL(slug, src))
  })
  return root.innerHTML
}

export function sanitizePublicHTMLWithPurifier(
  raw: string,
  purifier: DOMPurifyLike,
  document: Document,
  options: PublicContentOptions = {}
): string {
  const rewritten = rewriteRelativeHTMLImageSources(raw, document, options.pageSlug)
  const sanitized = purifier.sanitize(rewritten, {
    ALLOWED_TAGS: [...PUBLIC_CONTENT_ALLOWED_TAGS],
    ALLOWED_ATTR: [...PUBLIC_CONTENT_ALLOWED_ATTR],
    ALLOW_DATA_ATTR: false,
    FORBID_TAGS: ['script', 'style', 'iframe', 'object', 'embed', 'math', 'form', 'input', 'button', 'video', 'audio', 'source', 'details'],
    FORBID_ATTR: ['srcset'],
  })

  const root = document.createElement('div')
  root.innerHTML = sanitized
  postProcessSanitizedHTML(root)
  return root.innerHTML
}

export function sanitizePublicHTML(raw: string, options: PublicContentOptions = {}): string {
  return sanitizePublicHTMLWithPurifier(raw, getBrowserPurifier(), document, options)
}

export function renderPublicMarkdownWithPurifier(
  markdown: string,
  purifier: DOMPurifyLike,
  document: Document,
  options: PublicContentOptions = {}
): string {
  const html = marked.parse(markdown) as string
  return sanitizePublicHTMLWithPurifier(html, purifier, document, options)
}

export function renderPublicMarkdown(markdown: string, options: PublicContentOptions = {}): string {
  return renderPublicMarkdownWithPurifier(markdown, getBrowserPurifier(), document, options)
}
