import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { referralAPI } from '@/api/referral'
import type { CustomAffiliate, CustomReferralSummary } from '@/types'
import { useAuthStore } from './auth'

export const useReferralStore = defineStore('referral', () => {
  const profile = ref<CustomAffiliate | null>(null)
  const summary = ref<CustomReferralSummary | null>(null)
  const loading = ref(false)
  const loadedForUserId = ref<number | null>(null)
  const canAccess = computed(() => profile.value?.status === 'approved' || profile.value?.status === 'disabled')
  const canWithdraw = computed(() => profile.value?.status === 'approved' && profile.value.withdrawal_enabled)

  function clear(): void {
    profile.value = null
    summary.value = null
    loading.value = false
    loadedForUserId.value = null
  }

  async function ensureLoaded(force = false): Promise<CustomReferralSummary | null> {
    const authStore = useAuthStore()
    const userId = authStore.user?.id ?? null
    if (!authStore.isAuthenticated || authStore.isAdmin || !userId) {
      clear()
      return null
    }
    if (!force && summary.value && loadedForUserId.value === userId) {
      return summary.value
    }
    loading.value = true
    try {
      profile.value = await referralAPI.getProfile()
      if (profile.value?.status === 'approved' || profile.value?.status === 'disabled') {
        summary.value = await referralAPI.getSummary()
      } else {
        summary.value = null
      }
      loadedForUserId.value = userId
      return summary.value
    } catch {
      profile.value = null
      summary.value = null
      loadedForUserId.value = userId
      return null
    } finally {
      loading.value = false
    }
  }

  return {
    profile,
    summary,
    loading,
    canAccess,
    canWithdraw,
    clear,
    ensureLoaded
  }
})
