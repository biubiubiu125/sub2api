<template>
  <div class="card p-2">
    <div class="flex flex-wrap gap-2">
      <RouterLink
        v-for="item in items"
        :key="item.path"
        :to="item.path"
        :class="[
          'rounded-lg px-3 py-2 text-sm font-medium transition-colors',
          isActive(item.path)
            ? 'bg-primary-100 text-primary-700 dark:bg-primary-900/30 dark:text-primary-300'
            : 'text-gray-600 hover:bg-gray-100 dark:text-dark-300 dark:hover:bg-dark-700',
        ]"
      >
        {{ item.label }}
      </RouterLink>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRoute, RouterLink } from 'vue-router'

const route = useRoute()

const items = [
  { path: '/affiliate', label: '推广中心' },
  { path: '/affiliate/commissions', label: '佣金明细' },
  { path: '/affiliate/withdraw', label: '提现申请' },
  { path: '/affiliate/withdrawals', label: '提现记录' },
]

function isActive(path: string): boolean {
  return route.path === path
}
</script>
