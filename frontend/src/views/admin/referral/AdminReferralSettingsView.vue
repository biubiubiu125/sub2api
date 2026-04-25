<template>
  <AppLayout>
    <div class="space-y-6">
      <div>
        <h2 class="text-xl font-semibold text-gray-900 dark:text-white">推广分佣设置</h2>
        <p class="mt-1 text-sm text-gray-500 dark:text-dark-400">
          用于配置推广归因、冻结结算和提现规则，保存后立即生效。
        </p>
      </div>

      <div v-if="loading" class="flex items-center justify-center py-12">
        <LoadingSpinner />
      </div>

      <form v-else class="card space-y-6 p-6" @submit.prevent="saveSettings">
        <div class="grid gap-4 md:grid-cols-2">
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">返佣引擎</label>
            <Select v-model="form.provider" :options="providerOptions" />
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">Cookie 保留天数</label>
            <input v-model.number="form.cookie_ttl_days" type="number" min="0" step="1" class="input" />
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">全局返佣比例</label>
            <input v-model.number="form.default_rate" type="number" min="0" max="100" step="0.01" class="input" />
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">冻结结算天数</label>
            <input v-model.number="form.settle_freeze_days" type="number" min="0" step="1" class="input" />
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">最低提现金额</label>
            <input v-model.number="form.min_withdraw_amount" type="number" min="0" step="0.01" class="input" />
          </div>
          <div>
            <label class="mb-2 block text-sm font-medium text-gray-700 dark:text-gray-300">提现手续费</label>
            <input v-model.number="form.withdraw_fee" type="number" min="0" step="0.01" class="input" />
          </div>
        </div>

        <div class="rounded-lg border border-gray-200 bg-gray-50 px-4 py-3 text-sm text-gray-600 dark:border-dark-700 dark:bg-dark-900 dark:text-dark-300">
          未设置全局返佣比例时，即使启用了推广分佣模块，也不会为新订单生成佣金。
        </div>

        <div class="flex items-center justify-end gap-3">
          <button type="button" class="btn btn-secondary" @click="loadSettings">重置</button>
          <button type="submit" class="btn btn-primary" :disabled="saving">
            <Icon name="check" size="sm" />
            <span>{{ saving ? '保存中...' : '保存设置' }}</span>
          </button>
        </div>
      </form>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import Select from '@/components/common/Select.vue'
import Icon from '@/components/icons/Icon.vue'
import adminReferralAPI from '@/api/admin/referral'
import { useAppStore } from '@/stores/app'
import { extractApiErrorMessage } from '@/utils/apiError'

const appStore = useAppStore()
const loading = ref(false)
const saving = ref(false)

const form = reactive({
  provider: 'disabled',
  cookie_ttl_days: 30,
  default_rate: 0,
  settle_freeze_days: 15,
  min_withdraw_amount: 0,
  withdraw_fee: 0,
})

const providerOptions = [
  { value: 'disabled', label: '关闭' },
  { value: 'custom', label: '自定义模块' },
]

async function loadSettings(): Promise<void> {
  loading.value = true
  try {
    const data = await adminReferralAPI.getSettings()
    form.provider = data.provider || 'disabled'
    form.cookie_ttl_days = data.cookie_ttl_days
    form.default_rate = data.default_rate
    form.settle_freeze_days = data.settle_freeze_days
    form.min_withdraw_amount = data.min_withdraw_amount
    form.withdraw_fee = data.withdraw_fee
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '加载推广分佣设置失败'))
  } finally {
    loading.value = false
  }
}

async function saveSettings(): Promise<void> {
  saving.value = true
  try {
    const data = await adminReferralAPI.updateSettings({ ...form })
    form.provider = data.provider || 'disabled'
    form.cookie_ttl_days = data.cookie_ttl_days
    form.default_rate = data.default_rate
    form.settle_freeze_days = data.settle_freeze_days
    form.min_withdraw_amount = data.min_withdraw_amount
    form.withdraw_fee = data.withdraw_fee
    appStore.showSuccess('推广分佣设置已保存')
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, '保存推广分佣设置失败'))
  } finally {
    saving.value = false
  }
}

onMounted(() => {
  loadSettings()
})
</script>
