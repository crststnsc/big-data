import { useEffect, useState } from 'react'
import { getFacets } from '../api/client'
import type { FacetsResponse } from '../types'

export interface Filters {
  type: string[]
  language: string[]
  feed_category: string[]
  entity: string
  date_from: string
  date_to: string
}

export const EMPTY_FILTERS: Filters = {
  type: [],
  language: [],
  feed_category: [],
  entity: '',
  date_from: '',
  date_to: '',
}

function toggle(list: string[], value: string): string[] {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value]
}

export default function FilterPanel({
  filters,
  onChange,
}: {
  filters: Filters
  onChange: (filters: Filters) => void
}) {
  const [facets, setFacets] = useState<FacetsResponse | null>(null)

  useEffect(() => {
    getFacets().then(setFacets).catch(() => setFacets(null))
  }, [])

  return (
    <aside className="filter-panel">
      <div className="filter-group">
        <h4>Type</h4>
        {['gdelt_event', 'rss_article'].map((t) => (
          <label key={t}>
            <input
              type="checkbox"
              checked={filters.type.includes(t)}
              onChange={() => onChange({ ...filters, type: toggle(filters.type, t) })}
            />
            {t === 'gdelt_event' ? 'GDELT events' : 'RSS articles'}
          </label>
        ))}
      </div>

      {facets && facets.languages.length > 0 && (
        <div className="filter-group">
          <h4>Language</h4>
          {facets.languages.map((b) => (
            <label key={b.key}>
              <input
                type="checkbox"
                checked={filters.language.includes(b.key)}
                onChange={() => onChange({ ...filters, language: toggle(filters.language, b.key) })}
              />
              {b.key.toUpperCase()} ({b.count.toLocaleString()})
            </label>
          ))}
        </div>
      )}

      {facets && facets.feed_categories.length > 0 && (
        <div className="filter-group">
          <h4>Category</h4>
          {facets.feed_categories.map((b) => (
            <label key={b.key}>
              <input
                type="checkbox"
                checked={filters.feed_category.includes(b.key)}
                onChange={() =>
                  onChange({ ...filters, feed_category: toggle(filters.feed_category, b.key) })
                }
              />
              {b.key} ({b.count.toLocaleString()})
            </label>
          ))}
        </div>
      )}

      <div className="filter-group">
        <h4>Entity</h4>
        <input
          type="text"
          placeholder="Person, org, or place"
          value={filters.entity}
          onChange={(e) => onChange({ ...filters, entity: e.target.value })}
        />
      </div>

      <div className="filter-group">
        <h4>Date range</h4>
        <input
          type="date"
          value={filters.date_from}
          onChange={(e) => onChange({ ...filters, date_from: e.target.value })}
        />
        <input
          type="date"
          value={filters.date_to}
          onChange={(e) => onChange({ ...filters, date_to: e.target.value })}
        />
      </div>

      <button type="button" className="clear-filters" onClick={() => onChange(EMPTY_FILTERS)}>
        Clear filters
      </button>
    </aside>
  )
}
