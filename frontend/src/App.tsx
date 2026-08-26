import { Suspense, lazy } from 'react'
import { NavLink, Route, Routes } from 'react-router-dom'
import DetailPage from './pages/DetailPage'
import SearchPage from './pages/SearchPage'

// recharts + leaflet are heavy (~600kB combined) and unused by the core
// search flow -- load them only when their page is actually visited.
const DashboardPage = lazy(() => import('./pages/DashboardPage'))
const MapPage = lazy(() => import('./pages/MapPage'))

export default function App() {
  return (
    <div className="app">
      <header className="app-header">
        <h1>News Events Search</h1>
        <nav className="app-nav">
          <NavLink to="/" end className={({ isActive }) => (isActive ? 'active' : '')}>
            Search
          </NavLink>
          <NavLink to="/dashboard" className={({ isActive }) => (isActive ? 'active' : '')}>
            Dashboard
          </NavLink>
          <NavLink to="/map" className={({ isActive }) => (isActive ? 'active' : '')}>
            Map
          </NavLink>
        </nav>
      </header>
      <Suspense fallback={<p className="status-message">Loading…</p>}>
        <Routes>
          <Route path="/" element={<SearchPage />} />
          <Route path="/documents/:id" element={<DetailPage />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/map" element={<MapPage />} />
        </Routes>
      </Suspense>
    </div>
  )
}
