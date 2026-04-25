import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { referralAPI } from '@/api/referral'
import type { CustomReferralSummary } from '@/types'
import { useAuthStore } from './auth'

export const useReferralStore = defineStore('referral', () => {
  const summary = ref<CustomReferralSummary | null>(null)
  const loading = ref(false)
  const loadedForUserId = ref<number | null>(null)
  const canAccess = computed(() => !!summary.value)

  function clear(): void {
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
      const data = await referralAPI.getSummary()
      summary.value = data
      loadedForUserId.value = userId
      return data
    } catch {
      summary.value = null
      loadedForUserId.value = userId
      return null
    } finally {
      loading.value = false
    }
  }

  return {
    summary,
    loading,
    canAccess,
    clear,
    ensureLoaded
  }
})
