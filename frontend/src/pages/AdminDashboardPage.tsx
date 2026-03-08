import {
  Activity,
  ArrowRight,
  Database,
  ExternalLink,
  PlayCircle,
  ServerCog,
  Workflow,
} from 'lucide-react'
import { useEffect, useState } from 'react'
import { simulateTraffic, triggerMlPipeline } from '../api'
import { useAppContext } from '../context/AppContext'

export function AdminDashboardPage() {
  const { activeUserId, refreshRecommendations } = useAppContext()
  const [toast, setToast] = useState<string | null>(null)
  const [simulating, setSimulating] = useState<boolean>(false)
  const [triggeringPipeline, setTriggeringPipeline] = useState<boolean>(false)

  useEffect(() => {
    if (!toast) {
      return
    }

    const timer = window.setTimeout(() => setToast(null), 2500)
    return () => window.clearTimeout(timer)
  }, [toast])

  const handleTrafficSimulation = async () => {
    setSimulating(true)
    try {
      const result = await simulateTraffic({
        viewed_count: 100,
        purchased_count: 20,
        user_start: 2,
        user_end: 10,
        product_start: 1,
        product_end: 50,
      })
      setToast(`Synthetic traffic sent: ${result.total_events} events -> Kafka`)
      await refreshRecommendations()
    } catch {
      setToast('Traffic simulation failed. Check backend and Kafka.')
    } finally {
      setSimulating(false)
    }
  }

  const handlePipelineTrigger = async () => {
    setTriggeringPipeline(true)
    try {
      const result = await triggerMlPipeline()
      setToast(`Airflow DAG triggered: ${result.dag_run_id}`)
    } catch {
      setToast('Failed to trigger Airflow DAG. Check Airflow/API credentials.')
    } finally {
      setTriggeringPipeline(false)
    }
  }

  return (
    <section className="space-y-6">
      <div className="panel">
        <div className="flex items-center gap-2">
          <ServerCog size={18} className="text-ink" />
          <p className="section-title">Admin Dashboard</p>
        </div>
        <h2 className="mt-2 text-2xl font-bold text-ink">System Control Center</h2>
        <p className="mt-2 text-sm text-ink/70">
          Observe orchestration and graph infrastructure in one place. Active test user:{" "}
          <span className="font-semibold text-ink">{activeUserId.toUpperCase()}</span>
        </p>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <article className="panel lg:col-span-1">
          <div className="mb-4 flex items-center gap-2">
            <ExternalLink size={18} className="text-ember" />
            <h3 className="section-title">System Links</h3>
          </div>

          <div className="space-y-3">
            <a
              href="http://localhost:8080"
              target="_blank"
              rel="noreferrer"
              className="flex items-center justify-between rounded-xl border border-ink/20 bg-white px-4 py-3 text-sm font-semibold text-ink transition hover:border-ember/50 hover:bg-ember/5"
            >
              Open Apache Airflow (MLOps)
              <ExternalLink size={16} />
            </a>
            <a
              href="http://localhost:7474"
              target="_blank"
              rel="noreferrer"
              className="flex items-center justify-between rounded-xl border border-ink/20 bg-white px-4 py-3 text-sm font-semibold text-ink transition hover:border-ember/50 hover:bg-ember/5"
            >
              Open Neo4j Browser (Graph)
              <ExternalLink size={16} />
            </a>
          </div>
        </article>

        <article className="panel lg:col-span-2">
          <div className="mb-4 flex items-center gap-2">
            <Workflow size={18} className="text-ember" />
            <h3 className="section-title">System Architecture</h3>
          </div>

          <p className="text-sm text-ink/70">
            Data flow from user actions to machine-learning recommendations:
          </p>

          <div className="mt-4 grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-4">
            {[
              { label: 'React UI', icon: Activity },
              { label: 'FastAPI API', icon: ServerCog },
              { label: 'Kafka + Stream Processor', icon: ArrowRight },
              { label: 'Neo4j Graph', icon: Database },
              { label: 'Airflow + GDS', icon: Workflow },
              { label: 'Redis Cache', icon: Database },
              { label: 'Recommendations API', icon: ServerCog },
              { label: 'React UI Refresh', icon: Activity },
            ].map((step, index) => (
              <div
                key={`${step.label}-${index}`}
                className="rounded-xl border border-ink/15 bg-white px-3 py-3 text-xs font-semibold uppercase tracking-[0.08em] text-ink/75"
              >
                <div className="mb-2 flex items-center gap-2 text-ink">
                  <step.icon size={14} />
                  <span>Step {index + 1}</span>
                </div>
                <p className="normal-case tracking-normal text-sm font-medium text-ink">{step.label}</p>
              </div>
            ))}
          </div>
        </article>
      </div>

      <article className="panel">
        <div className="mb-4 flex items-center gap-2">
          <PlayCircle size={18} className="text-ember" />
          <h3 className="section-title">Traffic Simulator</h3>
        </div>
        <p className="mb-4 text-sm text-ink/70">
          Demo control for generating synthetic interaction bursts through Kafka.
        </p>
        <p className="mb-4 text-xs text-ink/60">
          Note: recommendations refresh after stream processing and the next GDS pipeline run updates Redis.
        </p>
        <button
          type="button"
          disabled={simulating}
          onClick={() => void handleTrafficSimulation()}
          className="inline-flex items-center gap-2 rounded-xl bg-ink px-4 py-2 text-sm font-semibold text-mist transition hover:bg-ink/90 disabled:cursor-wait disabled:opacity-75"
        >
          <PlayCircle size={16} />
          {simulating ? 'Generating...' : 'Generate Random Traffic'}
        </button>
        <button
          type="button"
          disabled={triggeringPipeline}
          onClick={() => void handlePipelineTrigger()}
          className="ml-3 inline-flex items-center gap-2 rounded-xl border border-ink/20 bg-white px-4 py-2 text-sm font-semibold text-ink transition hover:border-ember/50 hover:bg-ember/5 disabled:cursor-wait disabled:opacity-75"
        >
          <Workflow size={16} />
          {triggeringPipeline ? 'Triggering...' : 'Run GDS Pipeline Now'}
        </button>
      </article>

      {toast ? (
        <div className="fixed bottom-5 right-5 z-50 rounded-xl border border-ink/20 bg-ink px-4 py-3 text-sm font-semibold text-mist shadow-card">
          {toast}
        </div>
      ) : null}
    </section>
  )
}
