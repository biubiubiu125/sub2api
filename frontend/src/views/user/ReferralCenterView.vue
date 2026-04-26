<template>
  <AppLayout>
    <div class="space-y-6">
      <template v-if="isApproved">
        <ReferralNavTabs />

        <div v-if="loading" class="flex items-center justify-center py-12">
          <LoadingSpinner />
        </div>

        <template v-else-if="summary">
          <div
            v-if="profile?.status === 'disabled'"
            class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200"
          >
            已停用，仅可查看历史记录。
          </div>

          <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div class="card p-5">
              <div class="text-sm text-gray-500 dark:text-dark-400">推广链接打开次数</div>
              <div class="mt-2 text-2xl font-semibold text-gray-900 dark:text-white">{{ summary.click_count }}</div>
            </div>
            <div class="card p-5">
              <div class="text-sm text-gray-500 dark:text-dark-400">绑定用户数量</div>
              <div class="mt-2 text-2xl font-semibold text-gray-900 dark:text-white">{{ summary.bound_user_count }}</div>
            </div>
            <div class="card p-5">
              <div class="text-sm text-gray-500 dark:text-dark-400">有效付费用户数量</div>
              <div class="mt-2 text-2xl font-semibold text-gray-900 dark:text-white">{{ summary.paid_user_count }}</div>
            </div>
            <div class="card p-5">
              <div class="text-sm text-gray-500 dark:text-dark-400">当前返佣比例</div>
              <div class="mt-2 text-2xl font-semibold text-emerald-600 dark:text-emerald-400">
                {{ formatRate(summary.rate) }}
              </div>
            </div>
          </div>

          <div class="card p-6">
            <div class="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div class="space-y-4">
                <div>
                  <div class="text-sm text-gray-500 dark:text-dark-400">推广码</div>
                  <div class="mt-2 flex items-center gap-2 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 dark:border-dark-700 dark:bg-dark-900">
                    <code class="flex-1 text-sm font-semibold text-gray-900 dark:text-white">{{ summary.invite_code }}</code>
                    <button class="btn btn-secondary btn-sm" @click="copyText(summary.invite_code, '推广码已复制')">
                      <Icon name="copy" size="sm" />
                      <span>复制</span>
                    </button>
                  </div>
                </div>

                <div>
                  <div class="text-sm text-gray-500 dark:text-dark-400">推广链接</div>
                  <div class="mt-2 flex items-center gap-2 rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 dark:border-dark-700 dark:bg-dark-900">
                    <code class="flex-1 truncate text-sm text-gray-700 dark:text-gray-300">{{ inviteLink }}</code>
                    <button class="btn btn-secondary btn-sm" @click="copyText(inviteLink, '推广链接已复制')">
                      <Icon name="copy" size="sm" />
                      <span>复制</span>
                    </button>
                  </div>
                </div>
              </div>

              <div class="grid w-full gap-4 sm:grid-cols-3 lg:w-[32rem]">
                <div class="rounded-lg border border-gray-200 px-4 py-3 dark:border-dark-700">
                  <div class="text-sm text-gray-500 dark:text-dark-400">待结算佣金</div>
                  <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ formatMoney(summary.pending_amount) }}</div>
                </div>
                <div class="rounded-lg border border-gray-200 px-4 py-3 dark:border-dark-700">
                  <div class="text-sm text-gray-500 dark:text-dark-400">可提现佣金</div>
                  <div class="mt-2 text-xl font-semibold text-emerald-600 dark:text-emerald-400">{{ formatMoney(summary.available_amount) }}</div>
                </div>
                <div class="rounded-lg border border-gray-200 px-4 py-3 dark:border-dark-700">
                  <div class="text-sm text-gray-500 dark:text-dark-400">已提现金额</div>
                  <div class="mt-2 text-xl font-semibold text-gray-900 dark:text-white">{{ formatMoney(summary.withdrawn_amount) }}</div>
                </div>
              </div>
            </div>
          </div>

          <div class="grid gap-4 lg:grid-cols-3">
            <RouterLink to="/affiliate/commissions" class="card p-5 transition-colors hover:border-primary-400">
              <div class="flex items-center gap-3">
                <div class="flex h-10 w-10 items-center justify-center rounded-lg bg-primary-100 text-primary-600 dark:bg-primary-900/30 dark:text-primary-300">
                  <Icon name="chart" size="md" />
                </div>
                <div>
                  <div class="font-medium text-gray-900 dark:text-white">佣金明细</div>
                  <div class="text-sm text-gray-500 dark:text-dark-400">查看订单返佣记录与状态</div>
                </div>
              </div>
            </RouterLink>

            <RouterLink to="/affiliate/withdraw" class="card p-5 transition-colors hover:border-primary-400">
              <div class="flex items-center gap-3">
                <div class="flex h-10 w-10 items-center justify-center rounded-lg bg-emerald-100 text-emerald-600 dark:bg-emerald-900/30 dark:text-emerald-300">
                  <Icon name="dollar" size="md" />
                </div>
                <div>
                  <div class="font-medium text-gray-900 dark:text-white">提现申请</div>
                  <div class="text-sm text-gray-500 dark:text-dark-400">填写收款信息并提交申请</div>
                </div>
              </div>
            </RouterLink>

            <RouterLink to="/affiliate/withdrawals" class="card p-5 transition-colors hover:border-primary-400">
              <div class="flex items-center gap-3">
                <div class="flex h-10 w-10 items-center justify-center rounded-lg bg-amber-100 text-amber-600 dark:bg-amber-900/30 dark:text-amber-300">
                  <Icon name="document" size="md" />
                </div>
                <div>
                  <div class="font-medium text-gray-900 dark:text-white">提现记录</div>
                  <div class="text-sm text-gray-500 dark:text-dark-400">查看审核结果与打款状态</div>
                </div>
              </div>
            </RouterLink>
          </div>
        </template>
      </template>

      <template v-else>
        <div class="card p-6">
          <div class="max-w-2xl space-y-5">
            <div>
              <h2 class="text-xl font-semibold text-gray-900 dark:text-white">推广中心</h2>
              <p class="mt-1 text-sm text-gray-500 dark:text-dark-400">推广员需要管理员审批通过后才能使用推广功能和提现功能。</p>
            </div>

            <div
              v-if="profile?.status === 'pending'"
              class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200"
            >
              你的推广员申请正在审核中，请等待管理员处理。
            </div>

            <div
              v-else-if="profile?.status === 'rejected'"
              class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-900/40 dark:bg-red-900/20 dark:text-red-200"
            >
              你的推广员申请未通过。{{ profile.risk_reason ? `原因：${profile.risk_reason}` : '' }}
            </div>

            <div
              v-else-if="profile?.status === 'disabled'"
              class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200"
            >
              当前推广员资格已停用，如需恢复请联系管理员。
            </div>

            <form v-else class="space-y-4" @submit.prevent="submitApplication">
              <div>
                <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">申请说明（可选）</label>
                <textarea
                  v-model.trim="applicationForm.applicant_note"
                  rows="5"
                  class="input min-h-[140px]"
                  placeholder="可填写你的推广渠道、运营方式或其他说明，方便管理员审核。"
                />
              </div>
              <div class="flex justify-end">
                <button class="btn btn-primary" type="submit" :disabled="submitting">
                  <span>{{ submitting ? '提交中...' : '提交申请' }}</span>
                </button>
              </div>
            </form>
          </div>
        </div>
      </template>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { RouterLink } from 'vue-router'
import AppLayout from '@/components/layout/AppLayout.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import Icon from '@/components/icons/Icon.vue'
import ReferralNavTabs from '@/components/referral/ReferralNavTabs.vue'
import { referralAPI } from '@/api/referral'
import { useAppStore } from '@/stores/app'
import { useReferralStore } from '@/stores/referral'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const referralStore = useReferralStore()
const submitting = ref(false)
const applicationForm = reactive({
  applicant_note: '',
})

const loading = computed(() => referralStore.loading)
const summary = computed(() => referralStore.summary)
const profile = computed(() => referralStore.profile)
const isApproved = computed(() => profile.value?.status === 'approved')
const inviteLink = computed(() => {
  if (!summary.value || typeof window === 'undefined') return ''
  return `${window.location.origin}/r/${encodeURIComponent(summary.value.invite_code)}`
})

function formatMoney(value: number): string {
  return `¥${value.toFixed(2)}`
}

function formatRate(value?: number | null): string {
  if (value === null || value === undefined) return '未设置'
  return `${value}%`
}

async function copyText(value: string, message: string): Promise<void> {
  if (!value) return
  try {
    await navigator.clipboard.writeText(value)
    appStore.showSuccess(message)
  } catch {
    appStore.showError('复制失败')
  }
}

async function submitApplication(): Promise<void> {
  if (submitting.value) return
  submitting.value = true
  try {
    await referralAPI.applyAffiliate({
      applicant_note: applicationForm.applicant_note || '',
    })
    appStore.showSuccess('推广员申请已提交')
    applicationForm.applicant_note = ''
    await referralStore.ensureLoaded(true)
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '提交推广员申请失败'))
  } finally {
    submitting.value = false
  }
}

onMounted(() => {
  referralStore.ensureLoaded(true).catch(() => undefined)
})
</script>
