import type { FacetBucket } from '../../types'
import HorizontalBarChart from './HorizontalBarChart'

export default function LanguageChart({ data }: { data: FacetBucket[] }) {
  const sorted = [...data].sort((a, b) => b.count - a.count)
  return <HorizontalBarChart data={sorted} color="bar2" labelFor={(k) => k.toUpperCase()} />
}
