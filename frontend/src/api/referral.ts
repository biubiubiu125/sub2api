import { apiClient } from './client'
import type {
  CustomAffiliate,
  CustomReferralSummary,
  CustomReferralUserCommission,
  CustomReferralWithdrawal,
  PaginatedResponse
} from '@/types'

export interface ReferralWithdrawalCreateRequest {
  amount: number
  account_type: string
  account_name?: string
  account_no: string
  account_network?: string
  qr_image_url?: string
  applicant_note?: string
}

export interface ReferralApplicationRequest {
  applicant_note?: string
}

async function getProfile(): Promise<CustomAffiliate | null> {
  const { data } = await apiClient.get<CustomAffiliate | null>('/ext/referral/profile')
  return data
}

async function getSummary(): Promise<CustomReferralSummary> {
  const { data } = await apiClient.get<CustomReferralSummary>('/ext/referral/summary')
  return data
}

async function applyAffiliate(payload?: ReferralApplicationRequest): Promise<CustomAffiliate> {
  const { data } = await apiClient.post<CustomAffiliate>('/ext/referral/apply', payload ?? {})
  return data
}

async function listCommissions(params?: {
  page?: number
  page_size?: number
  status?: string
}): Promise<PaginatedResponse<CustomReferralUserCommission>> {
  const { data } = await apiClient.get<PaginatedResponse<CustomReferralUserCommission>>('/ext/referral/commissions', { params })
  return data
}

async function listWithdrawals(params?: {
  page?: number
  page_size?: number
  status?: string
}): Promise<PaginatedResponse<CustomReferralWithdrawal>> {
  const { data } = await apiClient.get<PaginatedResponse<CustomReferralWithdrawal>>('/ext/referral/withdrawals', { params })
  return data
}

async function createWithdrawal(payload: ReferralWithdrawalCreateRequest, idempotencyKey?: string): Promise<CustomReferralWithdrawal> {
  const { data } = await apiClient.post<CustomReferralWithdrawal>('/ext/referral/withdrawals', payload, {
    headers: idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : undefined
  })
  return data
}

async function cancelWithdrawal(id: number): Promise<CustomReferralWithdrawal> {
  const { data } = await apiClient.post<CustomReferralWithdrawal>(`/ext/referral/withdrawals/${id}/cancel`)
  return data
}

async function uploadAsset(file: File): Promise<{ url: string }> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await apiClient.post<{ url: string }>('/ext/referral/upload', formData)
  return data
}

export const referralAPI = {
  getProfile,
  getSummary,
  applyAffiliate,
  listCommissions,
  listWithdrawals,
  createWithdrawal,
  cancelWithdrawal,
  uploadAsset
}

export default referralAPI
