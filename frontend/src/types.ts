export type Product = {
  id: string
  name: string
  category: string
  price: number
}

export type Recommendation = Product & {
  score: number
  supporting_users: number
}

export type TrackAction = 'VIEWED' | 'PURCHASED'

export type SimulateTrafficRequest = {
  viewed_count?: number
  purchased_count?: number
  user_start?: number
  user_end?: number
  product_start?: number
  product_end?: number
  seed?: number
}

export type SimulateTrafficResponse = {
  status: string
  topic: string
  total_events: number
  viewed_events: number
  purchased_events: number
}

export type TriggerPipelineResponse = {
  status: string
  dag_id: string
  dag_run_id: string
  state?: string | null
  message: string
}
