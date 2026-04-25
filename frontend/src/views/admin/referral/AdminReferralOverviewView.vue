<template>
  <AppLayout>
    <div class="space-y-6">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 class="text-xl font-semibold text-gray-900 dark:text-white">推广分佣概览</h2>
          <p class="mt-1 text-sm text-gray-500 dark:text-dark-400">结算任务仅处理已过冻结期且仍为待结算状态的佣金记录。</p>
        </div>
        <button class="btn btn-primary" :disabled="running" @click="runSettlement">
          <Icon name="refresh" size="sm" :class="running ? 'animate-spin' : ''" />
          <span>{{ running ? '执行中...' : '执行结算' }}</span>
        </button>
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

        <div v-if="lastBatch" class="card p-5">
          <div class="mb-3 text-base font-medium text-gray-900 dark:text-white">最近一次结算</div>
          <div class="grid gap-4 md:grid-cols-3">
            <div>
              <div class="text-sm text-gray-500 dark:text-dark-400">批次号</div>
              <div class="mt-1 font-mono text-sm text-gray-900 dark:text-white">{{ lastBatch.batch_no }}</div>
            </div>
            <div>
              <div class="text-sm text-gray-500 dark:text-dark-400">执行状态</div>
              <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ lastBatch.status }}</div>
            </div>
            <div>
              <div class="text-sm text-gray-500 dark:text-dark-400">执行时间</div>
              <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ formatDateTime(lastBatch.started_at) }}</div>
            </div>
            <div>
              <div class="text-sm text-gray-500 dark:text-dark-400">扫描数量</div>
              <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ lastBatch.scanned_count }}</div>
            </div>
            <div>
              <div class="text-sm text-gray-500 dark:text-dark-400">结算数量</div>
              <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ lastBatch.settled_count }}</div>
            </div>
            <div>
              <div class="text-sm text-gray-500 dark:text-dark-400">跳过数量</div>
              <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ lastBatch.skipped_count }}</div>
            </div>
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
import Icon from '@/components/icons/Icon.vue'
import adminReferralAPI from '@/api/admin/referral'
import type { CustomReferralAdminOverview, CustomReferralSettlementBatch } from '@/types'
import { useAppStore } from '@/stores/app'
import { formatDateTime } from '@/utils/format'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const loading = ref(false)
const running = ref(false)
const overview = ref<CustomReferralAdminOverview | null>(null)
const lastBatch = ref<CustomReferralSettlementBatch | null>(null)

const cards = computed(() => {
  if (!overview.value) return []
  return [
    { label: '推广员总数', value: overview.value.total_affiliates },
    { label: '已批准推广员', value: overview.value.approved_affiliates },
    { label: '链接打开次数', value: overview.value.referral_click_count },
    { label: '绑定用户数量', value: overview.value.bound_user_count },
    { label: '有效付费用户数量', value: overview.value.effective_paid_user_count },
    { label: '待结算佣金', value: `¥${overview.value.pending_amount.toFixed(2)}` },
    { label: '可提现佣金', value: `¥${overview.value.available_amount.toFixed(2)}` },
    { label: '已提现金额', value: `¥${overview.value.withdrawn_amount.toFixed(2)}` },
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

async function runSettlement(): Promise<void> {
  running.value = true
  try {
    lastBatch.value = await adminReferralAPI.runSettlementBatch()
    appStore.showSuccess('结算任务已执行')
    await loadOverview()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '执行结算失败'))
  } finally {
    running.value = false
  }
}

onMounted(() => {
  loadOverview()
})
</script>
