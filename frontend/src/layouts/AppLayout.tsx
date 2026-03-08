import { LayoutDashboard, Store } from 'lucide-react'
import { NavLink, Outlet } from 'react-router-dom'
import { useAppContext } from '../context/AppContext'

function navLinkClass({ isActive }: { isActive: boolean }) {
  return [
    'inline-flex items-center gap-2 rounded-full px-4 py-2 text-sm font-semibold transition',
    isActive ? 'bg-ink text-mist' : 'text-ink/70 hover:bg-ink/10 hover:text-ink',
  ].join(' ')
}

export function AppLayout() {
  const { activeUserId, setActiveUserId, userOptions, recommendationsLoading } = useAppContext()

  return (
    <div className="mx-auto min-h-screen max-w-7xl px-4 pb-10 pt-6 sm:px-6 lg:px-8">
      <header className="panel mb-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <div className="rounded-xl bg-ink p-2 text-mist">
              <Store size={20} />
            </div>
            <div>
              <p className="section-title">Enterprise Recommender</p>
              <h1 className="text-xl font-bold tracking-tight text-ink">Graph Commerce Control Surface</h1>
            </div>
          </div>

          <nav className="flex flex-wrap items-center gap-2">
            <NavLink to="/" className={navLinkClass} end>
              <Store size={16} />
              Webshop
            </NavLink>
            <NavLink to="/admin" className={navLinkClass}>
              <LayoutDashboard size={16} />
              Admin Dashboard
            </NavLink>
          </nav>

          <div className="min-w-[220px]">
            <label htmlFor="active-user" className="mb-1 block text-xs font-semibold uppercase tracking-[0.12em] text-ink/70">
              Active User Selector
            </label>
            <div className="flex items-center gap-2">
              <select
                id="active-user"
                value={activeUserId}
                onChange={(event) => setActiveUserId(event.target.value)}
                className="w-full rounded-xl border border-ink/20 bg-white px-3 py-2 text-sm font-medium text-ink outline-none ring-0 transition focus:border-ember/70"
              >
                {userOptions.map((userId) => (
                  <option key={userId} value={userId}>
                    {userId.replace('_', ' ').toUpperCase()}
                  </option>
                ))}
              </select>
              <span className="text-xs font-semibold uppercase tracking-[0.1em] text-ink/60">
                {recommendationsLoading ? 'Syncing...' : 'Synced'}
              </span>
            </div>
          </div>
        </div>
      </header>

      <Outlet />
    </div>
  )
}
