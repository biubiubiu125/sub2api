<template>
  <AppLayout>
    <div class="space-y-4">
      <div class="card p-4">
        <div class="flex flex-wrap items-center gap-3">
          <div class="w-full sm:w-44">
            <Select v-model="filters.status" :options="statusOptions" @change="loadWithdrawals" />
          </div>
          <div class="flex items-center justify-end gap-2 sm:ml-auto">
            <button class="btn btn-secondary" :disabled="loading" @click="loadWithdrawals">
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
          暂无提现记录。
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full min-w-[880px] text-left text-sm">
            <thead>
              <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                <th class="px-4 py-3 font-medium">申请金额</th>
                <th class="px-4 py-3 font-medium">手续费</th>
                <th class="px-4 py-3 font-medium">实际打款金额</th>
                <th class="px-4 py-3 font-medium">收款方式</th>
                <th class="px-4 py-3 font-medium">状态</th>
                <th class="px-4 py-3 font-medium">申请时间</th>
                <th class="px-4 py-3 font-medium">处理时限</th>
                <th class="px-4 py-3 font-medium">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in items" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                <td class="px-4 py-3 text-gray-900 dark:text-white">{{ formatMoney(item.amount) }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatMoney(item.fee_amount) }}</td>
                <td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{{ formatMoney(item.net_amount) }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ accountTypeLabel(item) }}</td>
                <td class="px-4 py-3">
                  <span :class="statusClass(item.status)" class="rounded-full px-2.5 py-1 text-xs font-medium">{{ statusLabel(item.status) }}</span>
                </td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.submitted_at) }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.payout_deadline_at) || '-' }}</td>
                <td class="px-4 py-3">
                  <div class="flex flex-wrap items-center gap-2">
                    <button class="btn btn-secondary btn-sm" @click="openDetail(item)">详情</button>
                    <button
                      v-if="item.status === 'pending'"
                      class="btn btn-secondary btn-sm"
                      @click="cancelWithdrawal(item.id)"
                    >
                      撤回
                    </button>
                  </div>
                </td>
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

      <BaseDialog :show="detailVisible" title="提现详情" width="wide" @close="detailVisible = false">
        <div v-if="selectedItem" class="grid gap-4 md:grid-cols-2">
          <div>
            <div class="text-sm text-gray-500 dark:text-dark-400">申请金额</div>
            <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ formatMoney(selectedItem.amount) }}</div>
          </div>
          <div>
            <div class="text-sm text-gray-500 dark:text-dark-400">实际打款金额</div>
            <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ formatMoney(selectedItem.net_amount) }}</div>
          </div>
          <div>
            <div class="text-sm text-gray-500 dark:text-dark-400">收款方式</div>
            <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ accountTypeLabel(selectedItem) }}</div>
          </div>
          <div>
            <div class="text-sm text-gray-500 dark:text-dark-400">收款人姓名</div>
            <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ selectedItem.account_name || '-' }}</div>
          </div>
          <div class="md:col-span-2">
            <div class="text-sm text-gray-500 dark:text-dark-400">收款账号</div>
            <div class="mt-1 break-all text-sm text-gray-900 dark:text-white">{{ selectedItem.account_no || '-' }}</div>
          </div>
          <div class="md:col-span-2" v-if="selectedItem.contact_info">
            <div class="text-sm text-gray-500 dark:text-dark-400">联系方式</div>
            <div class="mt-1 text-sm text-gray-900 dark:text-white">{{ selectedItem.contact_info }}</div>
          </div>
          <div class="md:col-span-2" v-if="selectedItem.applicant_note">
            <div class="text-sm text-gray-500 dark:text-dark-400">备注说明</div>
            <div class="mt-1 whitespace-pre-wrap text-sm text-gray-900 dark:text-white">{{ selectedItem.applicant_note }}</div>
          </div>
          <div class="md:col-span-2" v-if="selectedItem.reject_reason">
            <div class="text-sm text-gray-500 dark:text-dark-400">拒绝原因</div>
            <div class="mt-1 whitespace-pre-wrap text-sm text-red-600 dark:text-red-400">{{ selectedItem.reject_reason }}</div>
          </div>
          <div class="md:col-span-2" v-if="selectedItem.admin_note">
            <div class="text-sm text-gray-500 dark:text-dark-400">处理备注</div>
            <div class="mt-1 whitespace-pre-wrap text-sm text-gray-900 dark:text-white">{{ selectedItem.admin_note }}</div>
          </div>
          <div class="md:col-span-2" v-if="selectedItem.qr_image_url">
            <div class="mb-2 text-sm text-gray-500 dark:text-dark-400">收款二维码</div>
            <img :src="selectedItem.qr_image_url" alt="" class="max-h-72 rounded-lg border border-gray-200 dark:border-dark-700" />
          </div>
          <div class="md:col-span-2" v-if="selectedItem.payment_proof_url">
            <div class="mb-2 text-sm text-gray-500 dark:text-dark-400">打款凭证</div>
            <img :src="selectedItem.payment_proof_url" alt="" class="max-h-72 rounded-lg border border-gray-200 dark:border-dark-700" />
          </div>
        </div>
      </BaseDialog>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import Pagination from '@/components/common/Pagination.vue'
import Select from '@/components/common/Select.vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import Icon from '@/components/icons/Icon.vue'
import { referralAPI } from '@/api/referral'
import type { CustomReferralWithdrawal } from '@/types'
import { useAppStore } from '@/stores/app'
import { formatDateTime } from '@/utils/format'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const loading = ref(false)
const items = ref<CustomReferralWithdrawal[]>([])
const selectedItem = ref<CustomReferralWithdrawal | null>(null)
const detailVisible = ref(false)
const filters = reactive({ status: '' })
const pagination = reactive({ page: 1, page_size: 20, total: 0 })

const statusOptions = computed(() => [
  { value: '', label: '全部状态' },
  { value: 'pending', label: '待审核' },
  { value: 'approved', label: '申请已通过，等待打款' },
  { value: 'paid', label: '已打款' },
  { value: 'rejected', label: '申请未通过' },
  { value: 'canceled', label: '已撤回' },
])

function formatMoney(value: number): string {
  return `¥${value.toFixed(2)}`
}

function statusLabel(value: string): string {
  if (value === 'pending') return '待审核'
  if (value === 'approved') return '申请已通过，等待打款'
  if (value === 'paid') return '已打款'
  if (value === 'rejected') return '申请未通过'
  if (value === 'canceled') return '已撤回'
  return value || '-'
}

function statusClass(value: string): string {
  if (value === 'paid') return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
  if (value === 'approved') return 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'
  if (value === 'rejected') return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
  if (value === 'canceled') return 'bg-gray-200 text-gray-700 dark:bg-dark-700 dark:text-dark-300'
  return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
}

function accountTypeLabel(item: CustomReferralWithdrawal): string {
  if (item.account_type === 'usdt') {
    return item.account_network ? `USDT / ${item.account_network}` : 'USDT'
  }
  if (item.account_type === 'alipay') return '支付宝'
  if (item.account_type === 'wechat') return '微信'
  return item.account_type || '-'
}

async function loadWithdrawals(): Promise<void> {
  loading.value = true
  try {
    const data = await referralAPI.listWithdrawals({
      page: pagination.page,
      page_size: pagination.page_size,
      status: filters.status || undefined,
    })
    items.value = data.items || []
    pagination.total = data.total || 0
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载提现记录失败'))
  } finally {
    loading.value = false
  }
}

async function cancelWithdrawal(id: number): Promise<void> {
  try {
    await referralAPI.cancelWithdrawal(id)
    appStore.showSuccess('提现申请已撤回')
    loadWithdrawals()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '撤回提现申请失败'))
  }
}

function openDetail(item: CustomReferralWithdrawal): void {
  selectedItem.value = item
  detailVisible.value = true
}

function handlePageChange(page: number): void {
  pagination.page = page
  loadWithdrawals()
}

function handlePageSizeChange(pageSize: number): void {
  pagination.page_size = pageSize
  pagination.page = 1
  loadWithdrawals()
}

onMounted(() => {
  loadWithdrawals()
})
</script>
