<template>
  <AppLayout>
    <div class="space-y-6">
      <div>
        <h2 class="text-xl font-semibold text-gray-900 dark:text-white">推广分佣概览</h2>
        <p class="mt-1 text-sm text-gray-500 dark:text-dark-400">用于查看推广员、返佣和提现整体情况。</p>
      </div>

      <div v-if="loading" class="flex items-center justify-center py-12">
        <LoadingSpinner />
      </div>

      <template v-else-if="overview">
        <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <div v-for="card in cards" :key="card.label" class="card p-5">
            <div class="text-sm text-gray-500 dark:text-dark-400">{{ card.label }}</div>
            <div class="mt-2 text-2xl font-semibold text-gray-900 dark:text-white">{{ card.value }}</div>
          </div>
        </div>
      </template>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import adminReferralAPI from '@/api/admin/referral'
import type { CustomReferralAdminOverview } from '@/types'
import { useAppStore } from '@/stores/app'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const loading = ref(false)
const overview = ref<CustomReferralAdminOverview | null>(null)

const cards = computed(() => {
  if (!overview.value) return []
  return [
    { label: '推广员总数', value: overview.value.total_affiliates },
    { label: '已批准推广员', value: overview.value.approved_affiliates },
    { label: '链接打开次数', value: overview.value.referral_click_count },
    { label: '绑定用户数量', value: overview.value.bound_user_count },
    { label: '有效付费用户数量', value: overview.value.effective_paid_user_count },
    { label: '待结算佣金', value: `￥${overview.value.pending_amount.toFixed(2)}` },
    { label: '可提现佣金', value: `￥${overview.value.available_amount.toFixed(2)}` },
    { label: '已提现金额', value: `￥${overview.value.withdrawn_amount.toFixed(2)}` },
  ]
})

async function loadOverview(): Promise<void> {
  loading.value = true
  try {
    overview.value = await adminReferralAPI.getOverview()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载推广分佣概览失败'))
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadOverview()
})
</script>
