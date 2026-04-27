<template>
  <AppLayout>
    <div class="space-y-4">
      <ReferralNavTabs />

      <div class="grid gap-6 lg:grid-cols-[1.2fr_0.8fr]">
        <div class="card p-6">
          <div class="mb-5">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-white">提现申请</h2>
            <p class="mt-1 text-sm text-gray-500 dark:text-dark-400">提现申请审核通过后，财务将在 48 小时内完成打款。</p>
          </div>

          <form class="space-y-4" @submit.prevent="submitWithdrawal">
            <div
              v-if="!canWithdraw"
              class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900/40 dark:bg-amber-900/20 dark:text-amber-200"
            >
              当前推广员状态不可提交提现申请。
            </div>
            <div class="grid gap-4 sm:grid-cols-2">
              <div>
                <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">提现金额</label>
                <input v-model.number="form.amount" type="number" min="0" step="0.01" class="input" />
              </div>
              <div>
                <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">收款方式</label>
                <Select v-model="form.account_type" :options="accountTypeOptions" />
              </div>
            </div>

            <div class="grid gap-4 sm:grid-cols-2">
              <div v-if="form.account_type !== 'usdt'">
                <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">收款人姓名</label>
                <input v-model.trim="form.account_name" type="text" class="input" />
              </div>
              <div :class="form.account_type === 'usdt' ? 'sm:col-span-2' : ''">
                <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">
                  {{ form.account_type === 'usdt' ? '收款账号' : '收款账号' }}
                </label>
                <input v-model.trim="form.account_no" type="text" class="input" />
              </div>
            </div>

            <div v-if="form.account_type === 'usdt'">
              <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">链类型</label>
              <Select v-model="form.account_network" :options="networkOptions" />
            </div>

            <div>
              <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">收款二维码（建议上传，提高财务打款效率）</label>
              <input type="file" accept="image/*" class="input" @change="handleQrFileChange" />
            </div>

            <div v-if="form.qr_image_url" class="rounded-lg border border-gray-200 p-4 dark:border-dark-700">
              <div class="mb-2 text-sm font-medium text-gray-700 dark:text-gray-300">收款二维码预览</div>
              <img :src="form.qr_image_url" alt="" class="max-h-56 rounded-lg border border-gray-200 dark:border-dark-700" />
            </div>

            <div>
              <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">备注说明</label>
              <textarea v-model.trim="form.applicant_note" rows="4" class="input min-h-[110px]"></textarea>
            </div>

            <div class="flex items-center justify-end gap-3">
              <RouterLink to="/affiliate/withdrawals" class="btn btn-secondary">查看提现记录</RouterLink>
              <button class="btn btn-primary" type="submit" :disabled="submitting || !canWithdraw">
                <Icon name="dollar" size="sm" />
                <span>{{ submitting ? '提交中...' : '提交申请' }}</span>
              </button>
            </div>
          </form>
        </div>

        <div class="space-y-4">
          <div class="card p-5">
            <div class="text-sm text-gray-500 dark:text-dark-400">可提现佣金</div>
            <div class="mt-2 text-2xl font-semibold text-emerald-600 dark:text-emerald-400">{{ formatMoney(summary?.available_amount ?? 0) }}</div>
          </div>
          <div class="card p-5">
            <div class="text-sm text-gray-500 dark:text-dark-400">待结算佣金</div>
            <div class="mt-2 text-2xl font-semibold text-gray-900 dark:text-white">{{ formatMoney(summary?.pending_amount ?? 0) }}</div>
          </div>
          <div class="card p-5">
            <div class="text-sm text-gray-500 dark:text-dark-400">已提现金额</div>
            <div class="mt-2 text-2xl font-semibold text-gray-900 dark:text-white">{{ formatMoney(summary?.withdrawn_amount ?? 0) }}</div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { reactive, computed, ref, onMounted } from 'vue'
import { RouterLink, useRouter } from 'vue-router'
import AppLayout from '@/components/layout/AppLayout.vue'
import Select from '@/components/common/Select.vue'
import Icon from '@/components/icons/Icon.vue'
import ReferralNavTabs from '@/components/referral/ReferralNavTabs.vue'
import { referralAPI } from '@/api/referral'
import { useAppStore } from '@/stores/app'
import { useReferralStore } from '@/stores/referral'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const referralStore = useReferralStore()
const router = useRouter()
const submitting = ref(false)

const form = reactive({
  amount: 0,
  account_type: 'alipay',
  account_name: '',
  account_no: '',
  account_network: 'TRC20',
  qr_image_url: '',
  applicant_note: '',
})

const summary = computed(() => referralStore.summary)
const canWithdraw = computed(() => referralStore.canWithdraw)

const accountTypeOptions = [
  { value: 'alipay', label: '支付宝' },
  { value: 'wechat', label: '微信' },
  { value: 'usdt', label: 'USDT' },
]

const networkOptions = [
  { value: 'TRC20', label: 'TRC20' },
  { value: 'BEP20', label: 'BEP20' },
  { value: 'Polygon', label: 'Polygon' },
]

function formatMoney(value: number): string {
  return `￥${value.toFixed(2)}`
}

function handleQrFileChange(event: Event): void {
  const target = event.target as HTMLInputElement
  const file = target.files?.[0]
  if (!file) return
  referralAPI.uploadAsset(file)
    .then((result) => {
      form.qr_image_url = result.url
      appStore.showSuccess('收款二维码已上传')
    })
    .catch((error) => {
      appStore.showError(extractApiErrorMessage(error, '上传收款二维码失败'))
    })
}

function createWithdrawalIdempotencyKey(): string {
  const cryptoApi = globalThis.crypto
  if (cryptoApi?.randomUUID) {
    return cryptoApi.randomUUID()
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`
}

async function submitWithdrawal(): Promise<void> {
  if (submitting.value || !canWithdraw.value) return
  submitting.value = true
  const idempotencyKey = createWithdrawalIdempotencyKey()
  try {
    await referralAPI.createWithdrawal({
      amount: form.amount,
      account_type: form.account_type,
      account_name: form.account_type === 'usdt' ? '' : form.account_name,
      account_no: form.account_no,
      account_network: form.account_type === 'usdt' ? form.account_network : '',
      qr_image_url: form.qr_image_url || '',
      applicant_note: form.applicant_note || '',
    }, idempotencyKey)
    appStore.showSuccess('提现申请已提交')
    await referralStore.ensureLoaded(true)
    router.push('/affiliate/withdrawals')
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '提交提现申请失败'))
  } finally {
    submitting.value = false
  }
}

onMounted(() => {
  referralStore.ensureLoaded().catch(() => undefined)
  form.amount = summary.value?.available_amount ?? 0
})
</script>
