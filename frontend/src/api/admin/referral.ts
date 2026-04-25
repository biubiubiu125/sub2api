import { apiClient } from '../client'
import type {
  CustomAffiliate,
  CustomReferralAdminConfig,
  CustomReferralAdminOverview,
  CustomReferralCommission,
  CustomReferralSettlementBatch,
  CustomReferralWithdrawal,
  PaginatedResponse
} from '@/types'

async function getOverview(): Promise<CustomReferralAdminOverview> {
  const { data } = await apiClient.get<CustomReferralAdminOverview>('/admin/referral/overview')
  return data
}

async function getSettings(): Promise<CustomReferralAdminConfig> {
  const { data } = await apiClient.get<CustomReferralAdminConfig>('/admin/referral/settings')
  return data
}

async function updateSettings(payload: {
  provider: string
  cookie_ttl_days: number
  default_rate: number
  settle_freeze_days: number
  min_withdraw_amount: number
  withdraw_fee: number
}): Promise<CustomReferralAdminConfig> {
  const { data } = await apiClient.put<CustomReferralAdminConfig>('/admin/referral/settings', payload)
  return data
}

async function listAffiliates(params?: {
  page?: number
  page_size?: number
  status?: string
  keyword?: string
}): Promise<PaginatedResponse<CustomAffiliate>> {
  const { data } = await apiClient.get<PaginatedResponse<CustomAffiliate>>('/admin/referral/affiliates', { params })
  return data
}

async function approveAffiliate(userId: number, payload?: { rate_override?: number | null }): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/approve`, payload ?? {})
  return data
}

async function disableAffiliate(userId: number, payload?: { reason?: string }): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/disable`, payload ?? {})
  return data
}

async function rejectAffiliate(userId: number, payload?: { reason?: string }): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/reject`, payload ?? {})
  return data
}

async function restoreAffiliate(userId: number): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/restore`)
  return data
}

async function freezeSettlement(userId: number, payload?: { reason?: string }): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/settlement/freeze`, payload ?? {})
  return data
}

async function restoreSettlement(userId: number): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/settlement/restore`)
  return data
}

async function freezeWithdrawal(userId: number, payload?: { reason?: string }): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/withdrawal/freeze`, payload ?? {})
  return data
}

async function restoreWithdrawal(userId: number): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>(`/admin/referral/affiliates/${userId}/withdrawal/restore`)
  return data
}

async function listCommissions(params?: {
  page?: number
  page_size?: number
  status?: string
}): Promise<PaginatedResponse<CustomReferralCommission>> {
  const { data } = await apiClient.get<PaginatedResponse<CustomReferralCommission>>('/admin/referral/commissions', { params })
  return data
}

async function runSettlementBatch(): Promise<CustomReferralSettlementBatch> {
  const { data } = await apiClient.post<CustomReferralSettlementBatch>('/admin/referral/settlements/run')
  return data
}

async function listWithdrawals(params?: {
  page?: number
  page_size?: number
  status?: string
}): Promise<PaginatedResponse<CustomReferralWithdrawal>> {
  const { data } = await apiClient.get<PaginatedResponse<CustomReferralWithdrawal>>('/admin/referral/withdrawals', { params })
  return data
}

async function approveWithdrawal(id: number, payload?: { admin_note?: string }): Promise<CustomReferralWithdrawal> {
  const { data } = await apiClient.post<CustomReferralWithdrawal>(`/admin/referral/withdrawals/${id}/approve`, payload ?? {})
  return data
}

async function rejectWithdrawal(id: number, payload: { admin_note?: string; reject_reason: string }): Promise<CustomReferralWithdrawal> {
  const { data } = await apiClient.post<CustomReferralWithdrawal>(`/admin/referral/withdrawals/${id}/reject`, payload)
  return data
}

async function markWithdrawalPaid(id: number, payload?: {
  admin_note?: string
  payment_proof_url?: string
  payment_txn_no?: string
}): Promise<CustomReferralWithdrawal> {
  const { data } = await apiClient.post<CustomReferralWithdrawal>(`/admin/referral/withdrawals/${id}/pay`, payload ?? {})
  return data
}

async function uploadAsset(file: File): Promise<{ url: string }> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await apiClient.post<{ url: string }>('/admin/referral/upload', formData)
  return data
}

export const adminReferralAPI = {
  getOverview,
  getSettings,
  updateSettings,
  listAffiliates,
  approveAffiliate,
  rejectAffiliate,
  disableAffiliate,
  restoreAffiliate,
  freezeSettlement,
  restoreSettlement,
  freezeWithdrawal,
  restoreWithdrawal,
  listCommissions,
  runSettlementBatch,
  listWithdrawals,
  approveWithdrawal,
  rejectWithdrawal,
  markWithdrawalPaid,
  uploadAsset
}

export default adminReferralAPI
