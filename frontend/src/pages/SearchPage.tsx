import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { searchDocuments } from '../api/client'
import FilterPanel, { type Filters } from '../components/FilterPanel'
import Pagination from '../components/Pagination'
import ResultList from '../components/ResultList'
import SearchBar from '../components/SearchBar'
import type { SearchResponse } from '../types'

const PAGE_SIZE = 20

function filtersFromParams(params: URLSearchParams): Filters {
  return {
    type: params.getAll('type'),
    language: params.getAll('language'),
    feed_category: params.getAll('feed_category'),
    entity: params.get('entity') ?? '',
    date_from: params.get('date_from') ?? '',
    date_to: params.get('date_to') ?? '',
  }
}

export default function SearchPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const q = searchParams.get('q') ?? ''
  const page = Number(searchParams.get('page') ?? '1')
  const filters = filtersFromParams(searchParams)

  const [response, setResponse] = useState<SearchResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    searchDocuments({
      q,
      page,
      page_size: PAGE_SIZE,
      type: filters.type.length ? filters.type : undefined,
      language: filters.language.length ? filters.language : undefined,
      feed_category: filters.feed_category.length ? filters.feed_category : undefined,
      entity: filters.entity || undefined,
      date_from: filters.date_from || undefined,
      date_to: filters.date_to || undefined,
    })
      .then(setResponse)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams.toString()])

  function updateParams(next: { q?: string; filters?: Filters; page?: number }) {
    const params = new URLSearchParams()
    const effectiveQ = next.q ?? q
    const effectiveFilters = next.filters ?? filters
    const effectivePage = next.page ?? 1 // any query/filter change resets to page 1

    if (effectiveQ) params.set('q', effectiveQ)
    for (const t of effectiveFilters.type) params.append('type', t)
    for (const l of effectiveFilters.language) params.append('language', l)
    for (const c of effectiveFilters.feed_category) params.append('feed_category', c)
    if (effectiveFilters.entity) params.set('entity', effectiveFilters.entity)
    if (effectiveFilters.date_from) params.set('date_from', effectiveFilters.date_from)
    if (effectiveFilters.date_to) params.set('date_to', effectiveFilters.date_to)
    if (effectivePage > 1) params.set('page', String(effectivePage))

    setSearchParams(params)
  }

  return (
    <div className="search-page">
      <SearchBar value={q} onSearch={(newQ) => updateParams({ q: newQ })} />
      <div className="search-layout">
        <FilterPanel filters={filters} onChange={(newFilters) => updateParams({ filters: newFilters })} />
        <main>
          {error && <p className="status-message error">Error: {error}</p>}
          {!error && response && (
            <>
              <ResultList results={response.results} total={response.total} loading={loading} />
              <Pagination
                page={page}
                pageSize={PAGE_SIZE}
                total={response.total}
                onPageChange={(newPage) => updateParams({ page: newPage })}
              />
            </>
          )}
        </main>
      </div>
    </div>
  )
}
