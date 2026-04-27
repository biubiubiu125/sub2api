<template>
  <AppLayout>
    <div class="space-y-4">
      <div class="card p-4">
        <div class="flex flex-wrap items-center gap-3">
          <div class="w-full sm:w-40">
            <Select v-model="filters.status" :options="statusOptions" @change="loadItems" />
          </div>
          <div class="flex items-center justify-end gap-2 sm:ml-auto">
            <button class="btn btn-secondary" :disabled="loading" @click="loadItems">
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
          暂无返佣记录
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full min-w-[1200px] text-left text-sm">
            <thead>
              <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                <th class="px-4 py-3 font-medium">推广员</th>
                <th class="px-4 py-3 font-medium">付费用户</th>
                <th class="px-4 py-3 font-medium">订单 ID</th>
                <th class="px-4 py-3 font-medium">订单类型</th>
                <th class="px-4 py-3 font-medium">可返佣金额</th>
                <th class="px-4 py-3 font-medium">佣金比例</th>
                <th class="px-4 py-3 font-medium">佣金金额</th>
                <th class="px-4 py-3 font-medium">状态</th>
                <th class="px-4 py-3 font-medium">操作</th>
                <th class="px-4 py-3 font-medium">创建时间</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in items" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.affiliate_email || `#${item.affiliate_user_id}` }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">
                  <div>{{ item.invitee_email || `#${item.invitee_user_id}` }}</div>
                  <div class="text-xs text-gray-500 dark:text-dark-400">{{ item.invitee_username || '-' }}</div>
                </td>
                <td class="px-4 py-3 font-mono text-gray-700 dark:text-gray-300">{{ item.order_id }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ orderTypeLabel(item.order_type) }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatMoney(netBaseAmount(item)) }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.rate }}%</td>
                <td class="px-4 py-3 font-medium text-gray-900 dark:text-white">
                  <div>{{ formatMoney(netCommissionAmount(item)) }}</div>
                  <div v-if="item.refunded_amount > 0" class="text-xs font-normal text-gray-500 dark:text-dark-400">已冲正 {{ formatMoney(item.refunded_amount) }}</div>
                </td>
                <td class="px-4 py-3">
                  <span :class="statusClass(item.status)" class="rounded-full px-2.5 py-1 text-xs font-medium">{{ statusLabel(item.status) }}</span>
                </td>
                <td class="px-4 py-3">
                  <button
                    class="inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50 dark:text-red-400 dark:hover:bg-red-900/20"
                    :disabled="netCommissionAmount(item) <= 0"
                    @click="openReverseDialog(item)"
                  >
                    <Icon name="refresh" size="sm" />
                    扣佣
                  </button>
                </td>
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

      <BaseDialog :show="reverseDialog.visible" title="手动扣佣 / 退款冲正" @close="closeReverseDialog">
        <div class="space-y-4">
          <div class="text-sm text-gray-600 dark:text-dark-300">
            订单 #{{ reverseDialog.target?.order_id || '-' }} / 佣金 #{{ reverseDialog.target?.id || '-' }}
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">退款金额 / 冲正基数</label>
            <input v-model.number="reverseDialog.refundAmount" type="number" min="0.01" step="0.01" class="input" />
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">原因</label>
            <textarea v-model.trim="reverseDialog.reason" rows="4" class="input min-h-[110px]"></textarea>
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">Idempotency-Key</label>
            <input v-model.trim="reverseDialog.idempotencyKey" type="text" class="input font-mono text-xs" />
          </div>
        </div>
        <template #footer>
          <button class="btn btn-secondary" @click="closeReverseDialog">取消</button>
          <button class="btn btn-danger" :disabled="reversing || !reverseDialog.reason.trim() || !reverseDialog.idempotencyKey.trim()" @click="submitReverseDialog">
            {{ reversing ? '提交中...' : '确认扣佣' }}
          </button>
        </template>
      </BaseDialog>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import Pagination from '@/components/common/Pagination.vue'
import Select from '@/components/common/Select.vue'
import Icon from '@/components/icons/Icon.vue'
import adminReferralAPI from '@/api/admin/referral'
import type { CustomReferralCommission } from '@/types'
import { useAppStore } from '@/stores/app'
import { formatDateTime } from '@/utils/format'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const loading = ref(false)
const reversing = ref(false)
const items = ref<CustomReferralCommission[]>([])
const filters = reactive({ status: '' })
const pagination = reactive({ page: 1, page_size: 20, total: 0 })
const reverseDialog = reactive<{
  visible: boolean
  target: CustomReferralCommission | null
  refundAmount: number
  reason: string
  idempotencyKey: string
}>({
  visible: false,
  target: null,
  refundAmount: 0,
  reason: '',
  idempotencyKey: '',
})

const statusOptions = computed(() => [
  { value: '', label: '全部状态' },
  { value: 'pending', label: '待结算' },
  { value: 'available', label: '可提现' },
  { value: 'reversed', label: '已冲正' },
])

function formatMoney(value: number): string {
  return `¥${value.toFixed(2)}`
}

function netCommissionAmount(item: CustomReferralCommission): number {
  return Math.max(0, item.commission_amount - (item.refunded_amount || 0))
}

function netBaseAmount(item: CustomReferralCommission): number {
  if (item.commission_amount <= 0) return Math.max(0, item.base_amount)
  return Math.max(0, item.base_amount * (netCommissionAmount(item) / item.commission_amount))
}

function orderTypeLabel(value: string): string {
  if (value === 'subscription') return '订阅'
  if (value === 'balance') return '余额充值'
  return value || '-'
}

function statusLabel(value: string): string {
  if (value === 'pending') return '待结算'
  if (value === 'available') return '可提现'
  if (value === 'reversed') return '已冲正'
  return value || '-'
}

function statusClass(value: string): string {
  if (value === 'available') return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
  if (value === 'reversed') return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
  return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
}

function newIdempotencyKey(prefix: string): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return `${prefix}-${crypto.randomUUID()}`
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

function openReverseDialog(item: CustomReferralCommission): void {
  reverseDialog.visible = true
  reverseDialog.target = item
  reverseDialog.refundAmount = Number(netBaseAmount(item).toFixed(2))
  reverseDialog.reason = ''
  reverseDialog.idempotencyKey = newIdempotencyKey('manual-reverse')
}

function closeReverseDialog(): void {
  reverseDialog.visible = false
  reverseDialog.target = null
  reverseDialog.refundAmount = 0
  reverseDialog.reason = ''
  reverseDialog.idempotencyKey = ''
}

async function submitReverseDialog(): Promise<void> {
  if (reversing.value || !reverseDialog.target) return
  if (!reverseDialog.refundAmount || reverseDialog.refundAmount <= 0) {
    appStore.showError('请输入有效退款金额')
    return
  }
  if (!reverseDialog.reason.trim()) {
    appStore.showError('请输入冲正原因')
    return
  }
  if (!reverseDialog.idempotencyKey) {
    appStore.showError('缺少 Idempotency-Key')
    return
  }
  if (!window.confirm('确认执行手动扣佣 / 退款冲正？')) {
    return
  }
  reversing.value = true
  try {
    await adminReferralAPI.reverseCommission({
      commission_id: reverseDialog.target.id,
      order_id: reverseDialog.target.order_id,
      refund_amount: reverseDialog.refundAmount,
      reason: reverseDialog.reason,
      idempotency_key: reverseDialog.idempotencyKey,
    })
    appStore.showSuccess('扣佣已完成')
    closeReverseDialog()
    await loadItems()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '扣佣失败'))
  } finally {
    reversing.value = false
  }
}

async function loadItems(): Promise<void> {
  loading.value = true
  try {
    const data = await adminReferralAPI.listCommissions({
      page: pagination.page,
      page_size: pagination.page_size,
      status: filters.status || undefined,
    })
    items.value = data.items || []
    pagination.total = data.total || 0
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载返佣记录失败'))
  } finally {
    loading.value = false
  }
}

function handlePageChange(page: number): void {
  pagination.page = page
  loadItems()
}

function handlePageSizeChange(pageSize: number): void {
  pagination.page_size = pageSize
  pagination.page = 1
  loadItems()
}

onMounted(() => {
  loadItems()
})
</script>
