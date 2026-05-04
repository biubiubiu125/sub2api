import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'

import AdminReferralSettingsView from '../referral/AdminReferralSettingsView.vue'

const { getSettings, updateSettings, showError, showSuccess } = vi.hoisted(() => ({
  getSettings: vi.fn(),
  updateSettings: vi.fn(),
  showError: vi.fn(),
  showSuccess: vi.fn(),
}))

vi.mock('@/api/admin/referral', () => ({
  default: {
    getSettings,
    updateSettings,
  },
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({
    showError,
    showSuccess,
  }),
}))

describe('AdminReferralSettingsView', () => {
  beforeEach(() => {
    getSettings.mockReset()
    updateSettings.mockReset()
    showError.mockReset()
    showSuccess.mockReset()
  })

  it('shows a load failure state and prevents save after settings request fails', async () => {
    getSettings.mockRejectedValue(new Error('network down'))

    const wrapper = mount(AdminReferralSettingsView, {
      global: {
        stubs: {
          AppLayout: { template: '<div><slot /></div>' },
          LoadingSpinner: true,
          Select: {
            props: ['modelValue', 'options'],
            emits: ['update:modelValue'],
            template: '<select />',
          },
          Icon: true,
        },
      },
    })

    await flushPromises()

    expect(wrapper.text()).toContain('推广分佣设置加载失败')
    expect(wrapper.text()).toContain('未展示真实配置')
    expect(wrapper.find('form').exists()).toBe(false)
    expect(updateSettings).not.toHaveBeenCalled()
  })
})
