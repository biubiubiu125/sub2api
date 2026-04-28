<template>
  <AppLayout>
    <div class="space-y-4">
      <ReferralNavTabs />

      <div class="card p-4">
        <div class="flex flex-wrap items-center gap-3">
          <div class="w-full sm:w-40">
            <Select v-model="filters.status" :options="statusOptions" @change="loadCommissions" />
          </div>
          <div class="flex items-center justify-end gap-2 sm:ml-auto">
            <button class="btn btn-secondary" :disabled="loading" @click="loadCommissions">
              <Icon name="refresh" size="sm" :class="loading ? 'animate-spin' : ''" />
            </button>
          </div>
        </div>
      </div>

      <div class="card overflow-hidden">
        <div v-if="loading" class="flex items-center justify-center py-12">
          <LoadingSpinner />
        </div>
        <div v-else-if="items.length === 0" class="px-6 py-12 text-center text-sm text-gray-500 dark:text-dark-400">
          暂无佣金记录。
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full min-w-[620px] text-left text-sm">
            <thead>
              <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                <th class="px-4 py-3 font-medium">订单类型</th>
                <th class="px-4 py-3 font-medium">佣金金额</th>
                <th class="px-4 py-3 font-medium">状态</th>
                <th class="px-4 py-3 font-medium">预计结算时间</th>
                <th class="px-4 py-3 font-medium">创建时间</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in items" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                <td class="px-4 py-3 text-gray-900 dark:text-white">{{ orderTypeLabel(item.order_type) }}</td>
                <td class="px-4 py-3 font-medium text-gray-900 dark:text-white">
                  <div>{{ formatMoney(netCommissionAmount(item)) }}</div>
                  <div v-if="item.refunded_amount > 0" class="text-xs font-normal text-gray-500 dark:text-dark-400">已冲销 {{ formatMoney(item.refunded_amount) }}</div>
                </td>
                <td class="px-4 py-3">
                  <span :class="statusClass(item.status)" class="rounded-full px-2.5 py-1 text-xs font-medium">{{ statusLabel(item.status) }}</span>
                </td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.available_at || item.settle_at) }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.created_at) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <Pagination
          v-if="pagination.total > 0"
          :page="pagination.page"
          :page-size="pagination.page_size"
          :total="pagination.total"
          @update:page="handlePageChange"
          @update:pageSize="handlePageSizeChange"
        />
      </div>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import Pagination from '@/components/common/Pagination.vue'
import Select from '@/components/common/Select.vue'
import Icon from '@/components/icons/Icon.vue'
import ReferralNavTabs from '@/components/referral/ReferralNavTabs.vue'
import { referralAPI } from '@/api/referral'
import type { CustomReferralUserCommission } from '@/types'
import { useAppStore } from '@/stores/app'
import { formatDateTime } from '@/utils/format'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const loading = ref(false)
const items = ref<CustomReferralUserCommission[]>([])
const filters = reactive({ status: '' })
const pagination = reactive({ page: 1, page_size: 20, total: 0 })

const statusOptions = computed(() => [
  { value: '', label: '全部状态' },
  { value: 'pending', label: '待结算' },
  { value: 'available', label: '可提现' },
  { value: 'reversed', label: '已冲销' },
])

function formatMoney(value: number): string {
  return `￥${value.toFixed(2)}`
}

function netCommissionAmount(item: CustomReferralUserCommission): number {
  return Math.max(0, item.commission_amount - (item.refunded_amount || 0))
}

function orderTypeLabel(value: string): string {
  if (value === 'subscription') return '订阅'
  if (value === 'balance') return '充值'
  return value || '-'
}

function statusLabel(value: string): string {
  if (value === 'pending') return '待结算'
  if (value === 'available') return '可提现'
  if (value === 'reversed') return '已冲销'
  return value || '-'
}

function statusClass(value: string): string {
  if (value === 'available') return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
  if (value === 'reversed') return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
  return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
}

async function loadCommissions(): Promise<void> {
  loading.value = true
  try {
    const data = await referralAPI.listCommissions({
      page: pagination.page,
      page_size: pagination.page_size,
      status: filters.status || undefined,
    })
    items.value = data.items || []
    pagination.total = data.total || 0
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载佣金记录失败'))
  } finally {
    loading.value = false
  }
}

function handlePageChange(page: number): void {
  pagination.page = page
  loadCommissions()
}

function handlePageSizeChange(pageSize: number): void {
  pagination.page_size = pageSize
  pagination.page = 1
  loadCommissions()
}

onMounted(() => {
  loadCommissions()
})
</script>
