<template>
  <AppLayout>
    <div class="space-y-4">
      <div class="card p-4">
        <div class="flex flex-wrap items-center gap-3">
          <input v-model.trim="filters.keyword" type="text" class="input w-full sm:w-64" placeholder="搜索邮箱、用户名或推广码" @keyup.enter="loadAffiliates" />
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
          <table class="w-full min-w-[980px] text-left text-sm">
            <thead>
              <tr class="border-b border-gray-200 bg-gray-50 text-gray-500 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-400">
                <th class="px-4 py-3 font-medium">账号</th>
                <th class="px-4 py-3 font-medium">推广码</th>
                <th class="px-4 py-3 font-medium">状态</th>
                <th class="px-4 py-3 font-medium">专属比例</th>
                <th class="px-4 py-3 font-medium">推广开关</th>
                <th class="px-4 py-3 font-medium">结算开关</th>
                <th class="px-4 py-3 font-medium">提现开关</th>
                <th class="px-4 py-3 font-medium">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in items" :key="item.id" class="border-b border-gray-100 last:border-b-0 dark:border-dark-800">
                <td class="px-4 py-3">
                  <div class="font-medium text-gray-900 dark:text-white">{{ item.email || '-' }}</div>
                  <div class="text-xs text-gray-500 dark:text-dark-400">{{ item.username || '-' }}</div>
                </td>
                <td class="px-4 py-3 font-mono text-gray-700 dark:text-gray-300">{{ item.invite_code }}</td>
                <td class="px-4 py-3">
                  <span :class="statusClass(item.status)" class="rounded-full px-2.5 py-1 text-xs font-medium">{{ statusLabel(item.status) }}</span>
                </td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.rate_override == null ? '-' : `${item.rate_override}%` }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.acquisition_enabled ? '开启' : '关闭' }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.settlement_enabled ? '开启' : '关闭' }}</td>
                <td class="px-4 py-3 text-gray-700 dark:text-gray-300">{{ item.withdrawal_enabled ? '开启' : '关闭' }}</td>
                <td class="px-4 py-3">
                  <div class="flex flex-wrap items-center gap-2">
                    <button v-if="item.status !== 'approved' && item.status !== 'rejected'" class="btn btn-secondary btn-sm" @click="openApproveDialog(item)">批准</button>
                    <button v-if="item.status === 'pending'" class="btn btn-secondary btn-sm" @click="rejectAffiliate(item)">驳回</button>
                    <button v-if="item.status === 'approved'" class="btn btn-secondary btn-sm" @click="openDisableDialog(item)">停用</button>
                    <button v-if="item.status === 'disabled'" class="btn btn-secondary btn-sm" @click="restoreAffiliate(item)">恢复</button>
                    <button
                      v-if="item.status === 'approved' && item.settlement_enabled"
                      class="btn btn-secondary btn-sm"
                      @click="toggleSettlement(item, false)"
                    >
                      冻结结算
                    </button>
                    <button
                      v-if="item.status === 'approved' && !item.settlement_enabled"
                      class="btn btn-secondary btn-sm"
                      @click="toggleSettlement(item, true)"
                    >
                      恢复结算
                    </button>
                    <button
                      v-if="item.status === 'approved' && item.withdrawal_enabled"
                      class="btn btn-secondary btn-sm"
                      @click="toggleWithdrawal(item, false)"
                    >
                      冻结提现
                    </button>
                    <button
                      v-if="item.status === 'approved' && !item.withdrawal_enabled"
                      class="btn btn-secondary btn-sm"
                      @click="toggleWithdrawal(item, true)"
                    >
                      恢复提现
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
import type { CustomAffiliate } from '@/types'
import { useAppStore } from '@/stores/app'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const loading = ref(false)
const submitting = ref(false)
const items = ref<CustomAffiliate[]>([])
const filters = reactive({ status: '', keyword: '' })
const pagination = reactive({ page: 1, page_size: 20, total: 0 })
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

const dialogTitle = computed(() => {
  if (dialog.mode === 'create') return '手动开通推广员'
  if (dialog.mode === 'approve') return '批准推广员'
  return '停用推广员'
})

const statusOptions = computed(() => [
  { value: '', label: '全部状态' },
  { value: 'approved', label: '已批准' },
  { value: 'disabled', label: '已停用' },
  { value: 'pending', label: '待处理' },
  { value: 'rejected', label: '已拒绝' },
])

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
      await adminReferralAPI.approveAffiliate(userId, {
        rate_override: dialog.rateOverride ?? undefined,
      })
      appStore.showSuccess(dialog.mode === 'create' ? '推广员已开通' : '推广员已批准')
    } else {
      await adminReferralAPI.disableAffiliate(target!.user_id, {
        reason: dialog.reason,
      })
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

async function toggleSettlement(item: CustomAffiliate, enabled: boolean): Promise<void> {
  try {
    if (enabled) {
      await adminReferralAPI.restoreSettlement(item.user_id)
      appStore.showSuccess('结算状态已恢复')
    } else {
      await adminReferralAPI.freezeSettlement(item.user_id, { reason: '管理员手动冻结结算' })
      appStore.showSuccess('结算状态已冻结')
    }
    loadAffiliates()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, enabled ? '恢复结算失败' : '冻结结算失败'))
  }
}

async function toggleWithdrawal(item: CustomAffiliate, enabled: boolean): Promise<void> {
  try {
    if (enabled) {
      await adminReferralAPI.restoreWithdrawal(item.user_id)
      appStore.showSuccess('提现状态已恢复')
    } else {
      await adminReferralAPI.freezeWithdrawal(item.user_id, { reason: '管理员手动冻结提现' })
      appStore.showSuccess('提现状态已冻结')
    }
    loadAffiliates()
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, enabled ? '恢复提现失败' : '冻结提现失败'))
  }
}

onMounted(() => {
  loadAffiliates()
})
</script>
