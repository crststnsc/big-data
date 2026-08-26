import { useEffect, useState } from 'react'
import { getStats } from '../api/client'
import EventRootCodeChart from '../components/charts/EventRootCodeChart'
import LanguageChart from '../components/charts/LanguageChart'
import QuadClassChart from '../components/charts/QuadClassChart'
import ToneChart from '../components/charts/ToneChart'
import VolumeChart from '../components/charts/VolumeChart'
import type { StatsResponse } from '../types'

export default function DashboardPage() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getStats().then(setStats).catch((err) => setError(err.message))
  }, [])

  if (error) return <p className="status-message error">Error: {error}</p>
  if (!stats) return <p className="status-message">Loading…</p>

  return (
    <div className="dashboard-page">
      <div className="stat-tiles">
        <div className="stat-tile">
          <div className="value">{stats.total_docs.toLocaleString()}</div>
          <div className="label">Total documents</div>
        </div>
        <div className="stat-tile">
          <div className="value">{stats.total_gdelt_events.toLocaleString()}</div>
          <div className="label">GDELT events</div>
        </div>
        <div className="stat-tile">
          <div className="value">{stats.total_rss_articles.toLocaleString()}</div>
          <div className="label">RSS articles</div>
        </div>
      </div>

      <div className="chart-grid">
        <div className="chart-section wide">
          <h3>Document volume over time</h3>
          <VolumeChart data={stats.volume_over_time} />
        </div>

        <div className="chart-section">
          <h3>GDELT event type (quad class)</h3>
          <QuadClassChart data={stats.quad_class} />
        </div>

        <div className="chart-section">
          <h3>GDELT top event categories</h3>
          <EventRootCodeChart data={stats.event_root_code} />
        </div>

        <div className="chart-section">
          <h3>RSS articles by language</h3>
          <LanguageChart data={stats.languages} />
        </div>

        <div className="chart-section wide">
          <h3>Average tone over time (GDELT)</h3>
          <ToneChart data={stats.avg_tone_over_time} />
        </div>
      </div>
    </div>
  )
}
