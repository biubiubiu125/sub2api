import { describe, expect, it } from 'vitest'
import { renderPublicMarkdown, sanitizePublicHTML } from '@/utils/publicContent.ts'

describe('public content utils', () => {
  it('sanitizes dangerous html while keeping safe formatting', () => {
    const html = sanitizePublicHTML(`
      <script>alert(1)</script>
      <p style="color:#0f766e" onclick="alert(1)">教程正文</p>
      <a href="https://example.com" target="_blank">查看完整中文接入说明</a>
      <img src="/api/v1/pages/tutorial/images/demo.png" onerror="alert(1)" alt="教程截图">
      <div style="position:fixed;top:0;left:0;width:100%;height:100%">overlay</div>
      <svg onload="alert(1)" viewBox="0 0 24 24"><circle cx="10" cy="10" r="10"></circle></svg>
      <img src="data:image/svg+xml,<svg onload=alert(1)>">
    `)

    expect(html).not.toContain('<script')
    expect(html).not.toContain('onclick=')
    expect(html).not.toContain('onerror=')
    expect(html).not.toContain('data:image/svg+xml')
    expect(html).not.toContain('position:fixed')
    expect(html).toContain('教程正文')
    expect(html).toContain('style="color: #0f766e"')
    expect(html).toContain('target="_blank"')
    expect(html).toContain('rel="noopener noreferrer nofollow"')
    expect(html).toContain('alt="教程截图"')
    expect(html).toContain('<svg')
    expect(html).toContain('viewBox="0 0 24 24"')
    expect(html).toContain('<circle cx="10" cy="10" r="10"></circle>')
  })

  it('renders markdown and rewrites safe inline formatting only', () => {
    const html = renderPublicMarkdown('# 教程文档\n\n**加粗** 与 [链接](https://example.com)')

    expect(html).toContain('<h1>教程文档</h1>')
    expect(html).toContain('<strong>加粗</strong>')
    expect(html).toContain('<a href="https://example.com" rel="noopener noreferrer nofollow">链接</a>')
  })

  it('preserves safe classes and inline svg structure needed by custom home content', () => {
    const html = sanitizePublicHTML(`
      <div class="hero-shell">
        <svg class="hero-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true">
          <path d="M4 12h16" stroke="currentColor" stroke-width="2" stroke-linecap="round"></path>
        </svg>
        <p class="hero-copy">首页正文</p>
      </div>
    `)

    expect(html).toContain('class="hero-shell"')
    expect(html).toContain('class="hero-icon"')
    expect(html).toContain('<svg')
    expect(html).toContain('viewBox="0 0 24 24"')
    expect(html).toContain('stroke="currentColor"')
    expect(html).toContain('stroke-linecap="round"')
    expect(html).toContain('class="hero-copy"')
  })
})
