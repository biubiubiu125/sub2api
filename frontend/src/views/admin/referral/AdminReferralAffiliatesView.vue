<template>
  <AppLayout>
    <div class="space-y-4">
      <div class="card p-4">
        <div class="flex flex-wrap items-center gap-3">
          <input
            v-model.trim="filters.keyword"
            type="text"
            class="input w-full sm:w-64"
            placeholder="搜索邮箱、用户名或推广码"
            @keyup.enter="loadAffiliates"
          />
          <div class="w-full sm:w-40">
            <Select v-model="filters.status" :options="statusOptions" @change="loadAffiliates" />
          </div>
          <div class="flex items-center justify-end gap-2 sm:ml-auto">
            <button class="btn btn-secondary" @click="openCreateDialog">手动开通</button>
            <button class="btn btn-secondary" :disabled="loading" @click="loadAffiliates">
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
          暂无推广员记录。
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full min-w-[920px] text-left text-sm">
            <thead>
              <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                <th class="px-4 py-3 font-medium">账号</th>
                <th class="px-4 py-3 font-medium">推广码</th>
                <th class="px-4 py-3 font-medium">状态</th>
                <th class="px-4 py-3 font-medium">专属比例</th>
                <th class="px-4 py-3 font-medium">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in items" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                <td class="px-4 py-3">
                  <button
                    type="button"
                    class="font-medium text-left text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300"
                    @click="openDetailDialog(item)"
                  >
                    {{ item.email || '-' }}
                  </button>
                  <div class="text-xs text-gray-500 dark:text-dark-400">{{ item.username || '-' }}</div>
                </td>
                <td class="px-4 py-3 font-mono text-gray-700 dark:text-gray-300">{{ item.invite_code }}</td>
                <td class="px-4 py-3">
                  <span :class="statusClass(item.status)" class="rounded-full px-2.5 py-1 text-xs font-medium">{{ statusLabel(item.status) }}</span>
                </td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.rate_override == null ? '-' : `${item.rate_override}%` }}</td>
                <td class="px-4 py-3">
                  <div class="flex flex-wrap items-center gap-2">
                    <button v-if="item.status !== 'approved' && item.status !== 'rejected'" class="btn btn-secondary btn-sm" @click="openApproveDialog(item)">批准</button>
                    <button v-if="item.status === 'pending'" class="btn btn-secondary btn-sm" @click="rejectAffiliate(item)">驳回</button>
                    <button v-if="item.status === 'approved'" class="btn btn-secondary btn-sm" @click="openDisableDialog(item)">停用</button>
                    <button v-if="item.status === 'disabled'" class="btn btn-secondary btn-sm" @click="restoreAffiliate(item)">恢复</button>
                    <button v-if="item.status === 'approved' || item.status === 'disabled'" class="btn btn-secondary btn-sm" @click="openAdjustDialog(item, 'increase')">加佣金</button>
                    <button v-if="item.status === 'approved' || item.status === 'disabled'" class="btn btn-secondary btn-sm" @click="openAdjustDialog(item, 'decrease')">减佣金</button>
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

      <BaseDialog :show="detailDialog.visible" title="推广员详情" width="extra-wide" @close="closeDetailDialog">
        <div v-if="detailDialog.target" class="space-y-5 text-sm">
          <div class="grid gap-4 sm:grid-cols-2">
            <div>
              <div class="text-xs text-gray-500 dark:text-dark-400">账号</div>
              <div class="mt-1 font-medium text-gray-900 dark:text-white">{{ detailDialog.target.email || '-' }}</div>
              <div class="text-xs text-gray-500 dark:text-dark-400">{{ detailDialog.target.username || '-' }}</div>
            </div>
            <div>
              <div class="text-xs text-gray-500 dark:text-dark-400">推广码</div>
              <div class="mt-1 font-mono text-gray-900 dark:text-white">{{ detailDialog.target.invite_code }}</div>
            </div>
          </div>

          <div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <button type="button" class="rounded-lg border border-gray-200 p-4 text-left dark:border-dark-700" @click="openBindingList">
              <div class="text-xs text-gray-500 dark:text-dark-400">绑定用户数量</div>
              <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ detailDialog.target.bound_user_count }}</div>
              <div class="mt-2 text-xs text-gray-500 dark:text-dark-400">点击查看详情</div>
            </button>
            <button type="button" class="rounded-lg border border-gray-200 p-4 text-left dark:border-dark-700" @click="openCommissionList">
              <div class="text-xs text-gray-500 dark:text-dark-400">有效付费用户数量</div>
              <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ detailDialog.target.paid_user_count }}</div>
              <div class="mt-2 text-xs text-gray-500 dark:text-dark-400">点击查看详情</div>
            </button>
            <div class="rounded-lg border border-gray-200 p-4 dark:border-dark-700">
              <div class="text-xs text-gray-500 dark:text-dark-400">推广链接打开次数</div>
              <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ detailDialog.target.click_count }}</div>
            </div>
            <div class="rounded-lg border border-gray-200 p-4 dark:border-dark-700">
              <div class="text-xs text-gray-500 dark:text-dark-400">待结算佣金</div>
              <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ formatMoney(detailDialog.target.pending_amount) }}</div>
            </div>
            <div class="rounded-lg border border-gray-200 p-4 dark:border-dark-700">
              <div class="text-xs text-gray-500 dark:text-dark-400">可提现佣金</div>
              <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ formatMoney(detailDialog.target.available_amount) }}</div>
            </div>
            <button type="button" class="rounded-lg border border-gray-200 p-4 text-left dark:border-dark-700" @click="openWithdrawalList">
              <div class="text-xs text-gray-500 dark:text-dark-400">已提现金额</div>
              <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ formatMoney(detailDialog.target.withdrawn_amount) }}</div>
              <div class="mt-2 text-xs text-gray-500 dark:text-dark-400">点击查看详情</div>
            </button>
          </div>
        </div>
        <template #footer>
          <button class="btn btn-secondary" @click="closeDetailDialog">关闭</button>
        </template>
      </BaseDialog>

      <BaseDialog :show="subDetail.visible" :title="subDetail.title" width="extra-wide" @close="closeSubDetail">
        <div class="space-y-4">
          <div v-if="subDetail.loading" class="flex items-center justify-center py-10">
            <LoadingSpinner />
          </div>

          <div v-else-if="subDetail.mode === 'bindings'">
            <div v-if="bindingItems.length === 0" class="py-10 text-center text-sm text-gray-500 dark:text-dark-400">暂无绑定用户记录。</div>
            <div v-else class="overflow-x-auto">
              <table class="w-full min-w-[720px] text-left text-sm">
                <thead>
                  <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                    <th class="px-4 py-3 font-medium">用户账号</th>
                    <th class="px-4 py-3 font-medium">用户名</th>
                    <th class="px-4 py-3 font-medium">绑定时间</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in bindingItems" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.invitee_email || `#${item.invitee_user_id}` }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.invitee_name || '-' }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.bound_at) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div v-else-if="subDetail.mode === 'commissions'">
            <div v-if="commissionItems.length === 0" class="py-10 text-center text-sm text-gray-500 dark:text-dark-400">暂无产生佣金的订单记录。</div>
            <div v-else class="overflow-x-auto">
              <table class="w-full min-w-[980px] text-left text-sm">
                <thead>
                  <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                    <th class="px-4 py-3 font-medium">付费用户</th>
                    <th class="px-4 py-3 font-medium">订单 ID</th>
                    <th class="px-4 py-3 font-medium">订单类型</th>
                    <th class="px-4 py-3 font-medium">有效付费金额</th>
                    <th class="px-4 py-3 font-medium">佣金比例</th>
                    <th class="px-4 py-3 font-medium">佣金金额</th>
                    <th class="px-4 py-3 font-medium">状态</th>
                    <th class="px-4 py-3 font-medium">创建时间</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in commissionItems" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">
                      <div>{{ item.invitee_email || `#${item.invitee_user_id}` }}</div>
                      <div class="text-xs text-gray-500 dark:text-dark-400">{{ item.invitee_username || '-' }}</div>
                    </td>
                    <td class="px-4 py-3 font-mono text-gray-700 dark:text-gray-300">{{ item.order_id }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ orderTypeLabel(item.order_type) }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatMoney(item.base_amount) }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.rate }}%</td>
                    <td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{{ formatMoney(item.commission_amount) }}</td>
                    <td class="px-4 py-3">
                      <span :class="commissionStatusClass(item.status)" class="rounded-full px-2.5 py-1 text-xs font-medium">{{ commissionStatusLabel(item.status) }}</span>
                    </td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.created_at) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div v-else>
            <div v-if="withdrawalItems.length === 0" class="py-10 text-center text-sm text-gray-500 dark:text-dark-400">暂无提现记录。</div>
            <div v-else class="overflow-x-auto">
              <table class="w-full min-w-[920px] text-left text-sm">
                <thead>
                  <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                    <th class="px-4 py-3 font-medium">申请金额</th>
                    <th class="px-4 py-3 font-medium">实际打款金额</th>
                    <th class="px-4 py-3 font-medium">收款方式</th>
                    <th class="px-4 py-3 font-medium">状态</th>
                    <th class="px-4 py-3 font-medium">申请时间</th>
                    <th class="px-4 py-3 font-medium">打款时间</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in withdrawalItems" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatMoney(item.amount) }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatMoney(item.net_amount) }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ accountTypeLabel(item) }}</td>
                    <td class="px-4 py-3">{{ withdrawalStatusLabel(item.status) }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.submitted_at) }}</td>
                    <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ formatDateTime(item.paid_at) || '-' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
        <template #footer>
          <button class="btn btn-secondary" @click="closeSubDetail">关闭</button>
        </template>
      </BaseDialog>

      <BaseDialog :show="dialog.visible" :title="dialogTitle" @close="closeDialog">
        <div class="space-y-4">
          <div v-if="dialog.mode === 'create'">
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">用户 ID</label>
            <input v-model.number="dialog.userId" type="number" min="1" step="1" class="input" />
            <p class="mt-1 text-xs text-gray-500 dark:text-dark-400">请填写需要开通推广员身份的用户 ID。</p>
          </div>
          <div v-else class="text-sm text-gray-600 dark:text-dark-300">
            {{ dialog.target?.email || dialog.target?.username || '-' }}
          </div>
          <div v-if="dialog.mode === 'approve' || dialog.mode === 'create'">
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">专属返佣比例（可选）</label>
            <input v-model.number="dialog.rateOverride" type="number" min="0" max="100" step="0.01" class="input" />
          </div>
          <div v-else>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">停用原因</label>
            <textarea v-model.trim="dialog.reason" rows="4" class="input min-h-[110px]"></textarea>
          </div>
        </div>
        <template #footer>
          <button class="btn btn-secondary" @click="closeDialog">取消</button>
          <button class="btn btn-primary" :disabled="submitting" @click="submitDialog">{{ submitting ? '提交中...' : '确认' }}</button>
        </template>
      </BaseDialog>

      <BaseDialog :show="adjustDialog.visible" :title="adjustDialogTitle" @close="closeAdjustDialog">
        <div class="space-y-4">
          <div class="text-sm text-gray-600 dark:text-dark-300">
            {{ adjustDialog.target?.email || adjustDialog.target?.username || '-' }}
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">调整金额</label>
            <input v-model.number="adjustDialog.amount" type="number" min="0.01" step="0.01" class="input" />
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">备注</label>
            <textarea v-model.trim="adjustDialog.remark" rows="4" class="input min-h-[110px]"></textarea>
          </div>
        </div>
        <template #footer>
          <button class="btn btn-secondary" @click="closeAdjustDialog">取消</button>
          <button class="btn btn-primary" :disabled="adjusting" @click="submitAdjustDialog">{{ adjusting ? '提交中...' : '确认' }}</button>
        </template>
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
import adminReferralAPI from '@/api/admin/referral'
import type { CustomAffiliate, CustomReferralBindingDetail, CustomReferralCommission, CustomReferralWithdrawal } from '@/types'
import { useAppStore } from '@/stores/app'
import { extractApiErrorMessage } from '@/utils/apiError'
import { formatDateTime } from '@/utils/format'

const appStore = useAppStore()
const loading = ref(false)
const submitting = ref(false)
const adjusting = ref(false)
const items = ref<CustomAffiliate[]>([])
const bindingItems = ref<CustomReferralBindingDetail[]>([])
const commissionItems = ref<CustomReferralCommission[]>([])
const withdrawalItems = ref<CustomReferralWithdrawal[]>([])
const filters = reactive({ status: '', keyword: '' })
const pagination = reactive({ page: 1, page_size: 20, total: 0 })
const detailDialog = reactive<{
  visible: boolean
  target: CustomAffiliate | null
}>({
  visible: false,
  target: null,
})
const subDetail = reactive<{
  visible: boolean
  loading: boolean
  mode: 'bindings' | 'commissions' | 'withdrawals'
  title: string
}>({
  visible: false,
  loading: false,
  mode: 'bindings',
  title: '',
})
const dialog = reactive<{
  visible: boolean
  mode: 'create' | 'approve' | 'disable'
  target: CustomAffiliate | null
  userId: number | null
  rateOverride: number | null
  reason: string
}>({
  visible: false,
  mode: 'create',
  target: null,
  userId: null,
  rateOverride: null,
  reason: '',
})
const adjustDialog = reactive<{
  visible: boolean
  mode: 'increase' | 'decrease'
  target: CustomAffiliate | null
  amount: number
  remark: string
}>({
  visible: false,
  mode: 'increase',
  target: null,
  amount: 0,
  remark: '',
})

const dialogTitle = computed(() => {
  if (dialog.mode === 'create') return '手动开通推广员'
  if (dialog.mode === 'approve') return '批准推广员'
  return '停用推广员'
})

const adjustDialogTitle = computed(() => (adjustDialog.mode === 'increase' ? '增加佣金' : '减少佣金'))

const statusOptions = computed(() => [
  { value: '', label: '全部状态' },
  { value: 'approved', label: '已批准' },
  { value: 'disabled', label: '已停用' },
  { value: 'pending', label: '待处理' },
  { value: 'rejected', label: '已拒绝' },
])

function formatMoney(value: number): string {
  return `¥${value.toFixed(2)}`
}

function statusLabel(value: string): string {
  if (value === 'approved') return '已批准'
  if (value === 'disabled') return '已停用'
  if (value === 'pending') return '待处理'
  if (value === 'rejected') return '已拒绝'
  return value || '-'
}

function statusClass(value: string): string {
  if (value === 'approved') return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
  if (value === 'disabled') return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
  return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
}

function orderTypeLabel(value: string): string {
  if (value === 'subscription') return '订阅'
  if (value === 'balance') return '充值'
  return value || '-'
}

function commissionStatusLabel(value: string): string {
  if (value === 'pending') return '待结算'
  if (value === 'available') return '可提现'
  if (value === 'reversed') return '已冲销'
  return value || '-'
}

function commissionStatusClass(value: string): string {
  if (value === 'available') return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
  if (value === 'reversed') return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
  return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
}

function withdrawalStatusLabel(value: string): string {
  if (value === 'pending') return '待审核'
  if (value === 'approved') return '申请已通过，等待打款'
  if (value === 'paid') return '已打款'
  if (value === 'rejected') return '申请未通过'
  if (value === 'canceled') return '已撤回'
  return value || '-'
}

function accountTypeLabel(item: CustomReferralWithdrawal): string {
  if (item.account_type === 'usdt') return item.account_network ? `USDT / ${item.account_network}` : 'USDT'
  if (item.account_type === 'alipay') return '支付宝'
  if (item.account_type === 'wechat') return '微信'
  return item.account_type || '-'
}

async function loadAffiliates(): Promise<void> {
  loading.value = true
  try {
    const data = await adminReferralAPI.listAffiliates({
      page: pagination.page,
      page_size: pagination.page_size,
      status: filters.status || undefined,
      keyword: filters.keyword || undefined,
    })
    items.value = data.items || []
    pagination.total = data.total || 0
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载推广员列表失败'))
  } finally {
    loading.value = false
  }
}

function handlePageChange(page: number): void {
  pagination.page = page
  loadAffiliates()
}

function handlePageSizeChange(pageSize: number): void {
  pagination.page_size = pageSize
  pagination.page = 1
  loadAffiliates()
}

function openDetailDialog(item: CustomAffiliate): void {
  detailDialog.visible = true
  detailDialog.target = item
}

function closeDetailDialog(): void {
  detailDialog.visible = false
  detailDialog.target = null
}

async function openBindingList(): Promise<void> {
  if (!detailDialog.target) return
  subDetail.visible = true
  subDetail.loading = true
  subDetail.mode = 'bindings'
  subDetail.title = '绑定用户详情'
  try {
    const data = await adminReferralAPI.listAffiliateBindings(detailDialog.target.user_id, { page: 1, page_size: 100 })
    bindingItems.value = data.items || []
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载绑定用户详情失败'))
  } finally {
    subDetail.loading = false
  }
}

async function openCommissionList(): Promise<void> {
  if (!detailDialog.target) return
  subDetail.visible = true
  subDetail.loading = true
  subDetail.mode = 'commissions'
  subDetail.title = '有效付费详情'
  try {
    const data = await adminReferralAPI.listCommissions({
      page: 1,
      page_size: 100,
      affiliate_user_id: detailDialog.target.user_id,
    })
    commissionItems.value = data.items || []
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载有效付费详情失败'))
  } finally {
    subDetail.loading = false
  }
}

async function openWithdrawalList(): Promise<void> {
  if (!detailDialog.target) return
  subDetail.visible = true
  subDetail.loading = true
  subDetail.mode = 'withdrawals'
  subDetail.title = '提现记录详情'
  try {
    const data = await adminReferralAPI.listWithdrawals({
      page: 1,
      page_size: 100,
      affiliate_user_id: detailDialog.target.user_id,
    })
    withdrawalItems.value = data.items || []
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载提现记录详情失败'))
  } finally {
    subDetail.loading = false
  }
}

function closeSubDetail(): void {
  subDetail.visible = false
  subDetail.loading = false
  bindingItems.value = []
  commissionItems.value = []
  withdrawalItems.value = []
}

function openApproveDialog(item: CustomAffiliate): void {
  dialog.visible = true
  dialog.mode = 'approve'
  dialog.target = item
  dialog.userId = item.user_id
  dialog.rateOverride = item.rate_override ?? null
  dialog.reason = ''
}

function openDisableDialog(item: CustomAffiliate): void {
  dialog.visible = true
  dialog.mode = 'disable'
  dialog.target = item
  dialog.userId = item.user_id
  dialog.rateOverride = item.rate_override ?? null
  dialog.reason = item.risk_reason || ''
}

function openCreateDialog(): void {
  dialog.visible = true
  dialog.mode = 'create'
  dialog.target = null
  dialog.userId = null
  dialog.rateOverride = null
  dialog.reason = ''
}

function closeDialog(): void {
  dialog.visible = false
  dialog.target = null
  dialog.userId = null
  dialog.reason = ''
  dialog.rateOverride = null
}

function openAdjustDialog(item: CustomAffiliate, mode: 'increase' | 'decrease'): void {
  adjustDialog.visible = true
  adjustDialog.mode = mode
  adjustDialog.target = item
  adjustDialog.amount = 0
  adjustDialog.remark = ''
}

function closeAdjustDialog(): void {
  adjustDialog.visible = false
  adjustDialog.target = null
  adjustDialog.amount = 0
  adjustDialog.remark = ''
}

async function submitDialog(): Promise<void> {
  if (submitting.value) return
  if (dialog.mode !== 'create' && !dialog.target) return
  const target = dialog.target
  submitting.value = true
  try {
    if (dialog.mode === 'approve' || dialog.mode === 'create') {
      const userId = dialog.mode === 'create' ? dialog.userId : target!.user_id
      if (!userId || userId <= 0) {
        appStore.showError('请填写有效的用户 ID')
        submitting.value = false
        return
      }
      await adminReferralAPI.approveAffiliate(userId, { rate_override: dialog.rateOverride ?? undefined })
      appStore.showSuccess(dialog.mode === 'create' ? '推广员已开通' : '推广员已批准')
    } else {
      await adminReferralAPI.disableAffiliate(target!.user_id, { reason: dialog.reason })
      appStore.showSuccess('推广员已停用')
    }
    closeDialog()
    loadAffiliates()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '处理推广员状态失败'))
  } finally {
    submitting.value = false
  }
}

async function submitAdjustDialog(): Promise<void> {
  if (adjusting.value || !adjustDialog.target) return
  if (!adjustDialog.amount || adjustDialog.amount <= 0) {
    appStore.showError('请输入有效的调整金额')
    return
  }
  adjusting.value = true
  try {
    const amount = adjustDialog.mode === 'increase' ? adjustDialog.amount : -adjustDialog.amount
    await adminReferralAPI.adjustAffiliate(adjustDialog.target.user_id, {
      amount,
      remark: adjustDialog.remark,
    })
    appStore.showSuccess(adjustDialog.mode === 'increase' ? '佣金已增加' : '佣金已减少')
    closeAdjustDialog()
    await loadAffiliates()
    if (detailDialog.target?.user_id === adjustDialog.target.user_id) {
      const updated = items.value.find((item) => item.user_id === adjustDialog.target?.user_id)
      if (updated) detailDialog.target = updated
    }
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '调整佣金失败'))
  } finally {
    adjusting.value = false
  }
}

async function restoreAffiliate(item: CustomAffiliate): Promise<void> {
  try {
    await adminReferralAPI.restoreAffiliate(item.user_id)
    appStore.showSuccess('推广员已恢复')
    loadAffiliates()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '恢复推广员失败'))
  }
}

async function rejectAffiliate(item: CustomAffiliate): Promise<void> {
  try {
    await adminReferralAPI.rejectAffiliate(item.user_id, { reason: '管理员驳回' })
    appStore.showSuccess('推广员已驳回')
    loadAffiliates()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '驳回推广员失败'))
  }
}

onMounted(() => {
  loadAffiliates()
})
</script>
