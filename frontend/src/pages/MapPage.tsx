import 'leaflet/dist/leaflet.css'
import { useEffect, useState } from 'react'
import { CircleMarker, MapContainer, Popup, TileLayer } from 'react-leaflet'
import { getMapEvents } from '../api/client'
import { eventRootCodeLabel } from '../cameo'
import { useChartPalette, quadClassMapColor, QUAD_CLASS_LABELS } from '../components/charts/palette'
import MapFilterPanel, { EMPTY_MAP_FILTERS } from '../components/MapFilterPanel'
import type { MapEventsResponse, MapFilters } from '../types'

function radiusForCount(count: number): number {
  return Math.min(4 + Math.sqrt(count), 30)
}

export default function MapPage() {
  const [filters, setFilters] = useState<MapFilters>(EMPTY_MAP_FILTERS)
  const [data, setData] = useState<MapEventsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const palette = useChartPalette()

  useEffect(() => {
    getMapEvents(filters).then(setData).catch((err) => setError(err.message))
  }, [filters])

  return (
    <div className="map-page">
      <div className="map-layout">
        <MapFilterPanel filters={filters} onChange={setFilters} />
        <main>
          {error && <p className="status-message error">Error: {error}</p>}
          {data && (
            <p className="result-count">
              {data.total_matched.toLocaleString()} events, shown as {data.bucket_count} location clusters
            </p>
          )}
          <div className="map-container">
            <MapContainer center={[20, 0]} zoom={2} style={{ height: '100%', width: '100%' }}>
              <TileLayer
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              />
              {data?.points.map((p) => (
                <CircleMarker
                  key={p.geohash}
                  center={[p.lat, p.lon]}
                  radius={radiusForCount(p.count)}
                  pathOptions={{ color: quadClassMapColor(p.quad_class, palette), fillOpacity: 0.6, weight: 1 }}
                >
                  <Popup>
                    <dl className="map-popup">
                      {p.place && (
                        <>
                          <dt>Place</dt>
                          <dd>
                            {p.place}
                            {p.country ? ` (${p.country})` : ''}
                          </dd>
                        </>
                      )}
                      {(p.actor1 || p.actor2) && (
                        <>
                          <dt>Actors</dt>
                          <dd>
                            {p.actor1 ?? 'Unknown'} → {p.actor2 ?? 'Unknown'}
                          </dd>
                        </>
                      )}
                      {p.event_code && (
                        <>
                          <dt>Event</dt>
                          <dd>{eventRootCodeLabel(p.event_code.slice(0, 2))}</dd>
                        </>
                      )}
                      {p.quad_class != null && (
                        <>
                          <dt>Class</dt>
                          <dd>{QUAD_CLASS_LABELS[String(p.quad_class)]}</dd>
                        </>
                      )}
                      {p.content_date && (
                        <>
                          <dt>Date</dt>
                          <dd>{new Date(p.content_date).toLocaleDateString()}</dd>
                        </>
                      )}
                      <dt>Events here</dt>
                      <dd>{p.count.toLocaleString()}</dd>
                    </dl>
                  </Popup>
                </CircleMarker>
              ))}
            </MapContainer>
          </div>
          <div className="map-legend">
            <span>
              <span className="swatch" style={{ background: palette.mapCooperation }} />
              Cooperation
            </span>
            <span>
              <span className="swatch" style={{ background: palette.mapConflict }} />
              Conflict
            </span>
            <span>Marker size = events in that area</span>
          </div>
        </main>
      </div>
    </div>
  )
}
