import axios from 'axios'
import type {
  Product,
  Recommendation,
  SimulateTrafficRequest,
  SimulateTrafficResponse,
  TriggerPipelineResponse,
  TrackAction,
} from './types'

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000',
  timeout: 10000,
})

export async function fetchProducts(limit = 50): Promise<Product[]> {
  const { data } = await api.get<Product[]>('/products', {
    params: { limit },
  })
  return data
}

export async function fetchRecommendations(userId: string, limit = 8): Promise<Recommendation[]> {
  const { data } = await api.get<Recommendation[]>(`/recommendations/${userId}`, {
    params: { limit },
  })
  return data
}

export async function trackProductAction(
  userId: string,
  productId: string,
  action: TrackAction,
): Promise<void> {
  await api.post('/track', {
    user_id: userId,
    product_id: productId,
    action,
  })
}

export async function simulateTraffic(
  payload: SimulateTrafficRequest = {},
): Promise<SimulateTrafficResponse> {
  const { data } = await api.post<SimulateTrafficResponse>('/admin/simulate-traffic', payload)
  return data
}

export async function triggerMlPipeline(): Promise<TriggerPipelineResponse> {
  const { data } = await api.post<TriggerPipelineResponse>('/admin/trigger-ml-pipeline')
  return data
}
