import { beforeEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'

import HomeView from '../HomeView.vue'

const authStore = {
  isAuthenticated: false,
  isAdmin: false,
  user: null as { email?: string } | null,
  checkAuth: vi.fn(),
}

const appStore = {
  cachedPublicSettings: {
    site_name: 'MyCustomSite',
    site_logo: '/logo.png',
    site_subtitle: 'Readable public home page',
    doc_url: '',
    home_content: '<h2>欢迎使用</h2><p>公开正文</p>',
  },
  siteName: 'MyCustomSite',
  siteLogo: '/logo.png',
  docUrl: '',
  publicSettingsLoaded: true,
  fetchPublicSettings: vi.fn(),
}

vi.mock('@/stores', () => ({
  useAuthStore: () => authStore,
  useAppStore: () => appStore,
}))

vi.mock('@/components/common/LocaleSwitcher.vue', () => ({
  default: {
    name: 'LocaleSwitcher',
    template: '<div />',
  },
}))

vi.mock('@/components/icons/Icon.vue', () => ({
  default: {
    name: 'Icon',
    template: '<span />',
  },
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string) => {
        if (key === 'common.login') return '登录'
        return key
      },
    }),
  }
})

describe('HomeView', () => {
  beforeEach(() => {
    authStore.isAuthenticated = false
    authStore.isAdmin = false
    authStore.user = null
    authStore.checkAuth.mockReset()
    appStore.fetchPublicSettings.mockReset()
    appStore.publicSettingsLoaded = true
    appStore.cachedPublicSettings = {
      site_name: 'MyCustomSite',
      site_logo: '/logo.png',
      site_subtitle: 'Readable public home page',
      doc_url: '',
      home_content: '<h2>欢迎使用</h2><p>公开正文</p>',
    }
    vi.stubGlobal('matchMedia', vi.fn().mockImplementation(() => ({
      matches: false,
      media: '',
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })))
  })

  it('renders html home content in full-page mode', () => {
    appStore.cachedPublicSettings = {
      site_name: 'MyCustomSite',
      site_logo: '/logo.png',
      site_subtitle: 'Readable public home page',
      doc_url: '',
      home_content: '<div class="hero-shell"><h2>欢迎使用</h2><p>公开正文</p></div>',
    }

    const wrapper = mount(HomeView, {
      global: {
        stubs: {
          RouterLink: {
            props: ['to'],
            template: '<a :href="to"><slot /></a>',
          },
        },
      },
    })

    expect(wrapper.html()).toContain('class="hero-shell"')
    expect(wrapper.html()).toContain('<h2>欢迎使用</h2>')
    expect(wrapper.html()).toContain('<p>公开正文</p>')
  })

  it('renders iframe mode when home content is an external URL', () => {
    appStore.cachedPublicSettings = {
      site_name: 'MyCustomSite',
      site_logo: '/logo.png',
      site_subtitle: 'Readable public home page',
      doc_url: '',
      home_content: 'https://example.com/embed',
    }

    const wrapper = mount(HomeView, {
      global: {
        stubs: {
          RouterLink: {
            props: ['to'],
            template: '<a :href="to"><slot /></a>',
          },
        },
      },
    })

    const iframe = wrapper.find('iframe')
    expect(iframe.exists()).toBe(true)
    expect(iframe.attributes('src')).toBe('https://example.com/embed')
  })
})
