import { Eye, RefreshCw, ShoppingCart, Sparkles } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { fetchProducts, trackProductAction } from '../api'
import { useAppContext } from '../context/AppContext'
import type { Product, TrackAction } from '../types'

function formatPrice(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(value)
}

type ToastState = {
  tone: 'success' | 'error'
  message: string
}

export function WebshopPage() {
  const {
    activeUserId,
    recommendations,
    recommendationsLoading,
    recommendationsError,
    refreshRecommendations,
  } = useAppContext()

  const [products, setProducts] = useState<Product[]>([])
  const [productsLoading, setProductsLoading] = useState<boolean>(false)
  const [productsError, setProductsError] = useState<string | null>(null)
  const [pendingActionKey, setPendingActionKey] = useState<string | null>(null)
  const [toast, setToast] = useState<ToastState | null>(null)

  const loadProducts = useCallback(async () => {
    setProductsLoading(true)
    setProductsError(null)

    try {
      const data = await fetchProducts(50)
      setProducts(data)
    } catch {
      setProducts([])
      setProductsError('Could not fetch product catalog from the backend.')
    } finally {
      setProductsLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadProducts()
  }, [loadProducts])

  useEffect(() => {
    if (!toast) {
      return
    }
    const timer = window.setTimeout(() => setToast(null), 2600)
    return () => window.clearTimeout(timer)
  }, [toast])

  const handleTrackAction = useCallback(
    async (productId: string, action: TrackAction) => {
      const actionKey = `${productId}:${action}`
      setPendingActionKey(actionKey)

      try {
        await trackProductAction(activeUserId, productId, action)
        setToast({ tone: 'success', message: 'Action tracked in Kafka' })
        await refreshRecommendations()
      } catch {
        setToast({ tone: 'error', message: 'Tracking failed. Check backend/Kafka health.' })
      } finally {
        setPendingActionKey(null)
      }
    },
    [activeUserId, refreshRecommendations],
  )

  const recommendationCountLabel = useMemo(
    () => `${recommendations.length} recommendation${recommendations.length === 1 ? '' : 's'}`,
    [recommendations.length],
  )

  return (
    <section className="space-y-6">
      <div className="panel relative overflow-hidden">
        <div className="pointer-events-none absolute -right-8 -top-10 h-40 w-40 rounded-full bg-ember/20 blur-2xl" />
        <div className="pointer-events-none absolute -bottom-10 -left-8 h-40 w-40 rounded-full bg-pine/20 blur-2xl" />

        <div className="relative flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <p className="section-title">Webshop</p>
            <h2 className="mt-1 text-3xl font-bold text-ink">Welcome back, {activeUserId.toUpperCase()}</h2>
            <p className="mt-2 max-w-2xl text-sm text-ink/70">
              Recommendations are served from Redis cache, trained by the Graph ML pipeline orchestrated in Airflow.
            </p>
          </div>
          <button
            type="button"
            onClick={() => void refreshRecommendations()}
            className="inline-flex items-center gap-2 rounded-xl bg-ink px-4 py-2 text-sm font-semibold text-mist transition hover:bg-ink/90"
          >
            <RefreshCw size={16} />
            Refresh Recommendations
          </button>
        </div>
      </div>

      <div className="panel">
        <div className="mb-4 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Sparkles size={18} className="text-ember" />
            <h3 className="section-title">Recommended For You</h3>
          </div>
          <span className="text-sm font-medium text-ink/70">{recommendationCountLabel}</span>
        </div>

        {recommendationsLoading ? (
          <p className="text-sm text-ink/70">Loading recommendations...</p>
        ) : recommendationsError ? (
          <p className="text-sm text-ember">{recommendationsError}</p>
        ) : recommendations.length === 0 ? (
          <p className="rounded-xl border border-dashed border-ink/20 p-4 text-sm text-ink/70">
            No recommendations yet for this user.
          </p>
        ) : (
          <div className="flex gap-4 overflow-x-auto pb-2">
            {recommendations.map((item) => (
              <article key={item.id} className="min-w-[260px] rounded-2xl border border-ink/10 bg-white p-4 shadow-sm">
                <div className="mb-2 flex items-start justify-between gap-2">
                  <p className="text-xs font-semibold uppercase tracking-[0.12em] text-pine/80">{item.category}</p>
                  <span className="rounded-full bg-ember/10 px-2 py-1 text-xs font-bold text-ember">
                    Score {item.score.toFixed(3)}
                  </span>
                </div>
                <p className="line-clamp-2 min-h-[48px] text-base font-semibold text-ink">{item.name}</p>
                <div className="mt-4 flex items-center justify-between">
                  <p className="text-lg font-bold text-pine">{formatPrice(item.price)}</p>
                  <p className="text-xs text-ink/60">{item.supporting_users} similar users</p>
                </div>
              </article>
            ))}
          </div>
        )}
      </div>

      <div className="panel">
        <div className="mb-4 flex items-center justify-between">
          <h3 className="section-title">All Products</h3>
          <span className="text-sm font-medium text-ink/70">
            {productsLoading ? 'Loading...' : `${products.length} items`}
          </span>
        </div>

        {productsError ? (
          <p className="text-sm text-ember">{productsError}</p>
        ) : (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
            {products.map((product) => {
              const isViewing = pendingActionKey === `${product.id}:VIEWED`
              const isBuying = pendingActionKey === `${product.id}:PURCHASED`
              const isBusy = isViewing || isBuying

              return (
                <article
                  key={product.id}
                  className="rounded-2xl border border-ink/10 bg-white p-4 transition hover:-translate-y-0.5 hover:border-ember/40 hover:shadow-card"
                >
                  <p className="text-xs font-semibold uppercase tracking-[0.12em] text-pine/80">{product.category}</p>
                  <h4 className="mt-2 line-clamp-2 min-h-[52px] text-lg font-semibold text-ink">{product.name}</h4>
                  <p className="mt-4 text-xl font-bold text-pine">{formatPrice(product.price)}</p>

                  <div className="mt-5 grid grid-cols-2 gap-2">
                    <button
                      type="button"
                      disabled={isBusy}
                      onClick={() => void handleTrackAction(product.id, 'VIEWED')}
                      className="inline-flex items-center justify-center gap-2 rounded-xl border border-ink/20 px-3 py-2 text-sm font-semibold text-ink transition hover:border-ink/40 disabled:cursor-wait disabled:opacity-70"
                    >
                      <Eye size={15} />
                      {isViewing ? 'Tracking...' : 'View Details'}
                    </button>
                    <button
                      type="button"
                      disabled={isBusy}
                      onClick={() => void handleTrackAction(product.id, 'PURCHASED')}
                      className="inline-flex items-center justify-center gap-2 rounded-xl bg-ink px-3 py-2 text-sm font-semibold text-mist transition hover:bg-ink/90 disabled:cursor-wait disabled:opacity-70"
                    >
                      <ShoppingCart size={15} />
                      {isBuying ? 'Tracking...' : 'Buy Now'}
                    </button>
                  </div>
                </article>
              )
            })}
          </div>
        )}
      </div>

      {toast ? (
        <div
          className={[
            'fixed bottom-5 right-5 z-50 rounded-xl border px-4 py-3 text-sm font-semibold shadow-card',
            toast.tone === 'success'
              ? 'border-pine/30 bg-pine/90 text-mist'
              : 'border-ember/30 bg-ember/90 text-mist',
          ].join(' ')}
        >
          {toast.message}
        </div>
      ) : null}
    </section>
  )
}
