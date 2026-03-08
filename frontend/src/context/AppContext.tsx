import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react'
import { fetchRecommendations } from '../api'
import type { Recommendation } from '../types'

type AppContextValue = {
  activeUserId: string
  setActiveUserId: (userId: string) => void
  userOptions: string[]
  recommendations: Recommendation[]
  recommendationsLoading: boolean
  recommendationsError: string | null
  refreshRecommendations: () => Promise<void>
}

const AppContext = createContext<AppContextValue | undefined>(undefined)

const USER_OPTIONS = Array.from({ length: 10 }, (_, i) => `user_${i + 1}`)

export function AppContextProvider({ children }: { children: ReactNode }) {
  const [activeUserId, setActiveUserId] = useState<string>('user_1')
  const [recommendations, setRecommendations] = useState<Recommendation[]>([])
  const [recommendationsLoading, setRecommendationsLoading] = useState<boolean>(false)
  const [recommendationsError, setRecommendationsError] = useState<string | null>(null)

  const refreshRecommendations = useCallback(async () => {
    setRecommendationsLoading(true)
    setRecommendationsError(null)

    try {
      const data = await fetchRecommendations(activeUserId, 10)
      setRecommendations(data)
    } catch {
      setRecommendations([])
      setRecommendationsError('Could not fetch recommendations from the API cache.')
    } finally {
      setRecommendationsLoading(false)
    }
  }, [activeUserId])

  useEffect(() => {
    void refreshRecommendations()
  }, [refreshRecommendations])

  const value = useMemo<AppContextValue>(
    () => ({
      activeUserId,
      setActiveUserId,
      userOptions: USER_OPTIONS,
      recommendations,
      recommendationsLoading,
      recommendationsError,
      refreshRecommendations,
    }),
    [activeUserId, recommendations, recommendationsLoading, recommendationsError, refreshRecommendations],
  )

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>
}

export function useAppContext(): AppContextValue {
  const context = useContext(AppContext)
  if (!context) {
    throw new Error('useAppContext must be used within AppContextProvider')
  }
  return context
}
