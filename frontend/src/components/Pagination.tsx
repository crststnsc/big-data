export default function Pagination({
  page,
  pageSize,
  total,
  onPageChange,
}: {
  page: number
  pageSize: number
  total: number
  onPageChange: (page: number) => void
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize))
  if (totalPages <= 1) return null

  return (
    <div className="pagination">
      <button disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
        ← Prev
      </button>
      <span>
        Page {page} of {totalPages.toLocaleString()}
      </span>
      <button disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>
        Next →
      </button>
    </div>
  )
}
