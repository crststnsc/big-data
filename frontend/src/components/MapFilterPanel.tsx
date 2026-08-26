import { EVENT_ROOT_CODE_LABELS } from '../cameo'
import type { MapFilters } from '../types'
import { QUAD_CLASS_LABELS } from './charts/palette'

export const EMPTY_MAP_FILTERS: MapFilters = { date_from: '', date_to: '', quad_class: [], event_root_code: [] }

function toggleNum(list: number[], value: number): number[] {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value]
}

function toggleStr(list: string[], value: string): string[] {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value]
}

export default function MapFilterPanel({
  filters,
  onChange,
}: {
  filters: MapFilters
  onChange: (filters: MapFilters) => void
}) {
  const quadClass = filters.quad_class ?? []
  const eventRootCode = filters.event_root_code ?? []

  return (
    <aside className="filter-panel">
      <div className="filter-group">
        <h4>Event type</h4>
        {Object.entries(QUAD_CLASS_LABELS).map(([key, label]) => (
          <label key={key}>
            <input
              type="checkbox"
              checked={quadClass.includes(Number(key))}
              onChange={() => onChange({ ...filters, quad_class: toggleNum(quadClass, Number(key)) })}
            />
            {label}
          </label>
        ))}
      </div>

      <div className="filter-group">
        <h4>Category</h4>
        {Object.entries(EVENT_ROOT_CODE_LABELS).map(([code, label]) => (
          <label key={code}>
            <input
              type="checkbox"
              checked={eventRootCode.includes(code)}
              onChange={() => onChange({ ...filters, event_root_code: toggleStr(eventRootCode, code) })}
            />
            {label}
          </label>
        ))}
      </div>

      <div className="filter-group">
        <h4>Date range</h4>
        <input
          type="date"
          value={filters.date_from ?? ''}
          onChange={(e) => onChange({ ...filters, date_from: e.target.value })}
        />
        <input
          type="date"
          value={filters.date_to ?? ''}
          onChange={(e) => onChange({ ...filters, date_to: e.target.value })}
        />
      </div>

      <button type="button" className="clear-filters" onClick={() => onChange(EMPTY_MAP_FILTERS)}>
        Clear filters
      </button>
    </aside>
  )
}
