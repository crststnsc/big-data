import type { DocumentSummary } from '../types'
import ResultCard from './ResultCard'

export default function ResultList({
  results,
  total,
  loading,
}: {
  results: DocumentSummary[]
  total: number
  loading: boolean
}) {
  if (loading) return <p className="status-message">Searching…</p>
  if (results.length === 0) return <p className="status-message">No results found.</p>

  return (
    <div>
      <p className="result-count">{total.toLocaleString()} result{total === 1 ? '' : 's'}</p>
      <div className="result-list">
        {results.map((doc) => (
          <ResultCard key={doc.id} doc={doc} />
        ))}
      </div>
    </div>
  )
}
