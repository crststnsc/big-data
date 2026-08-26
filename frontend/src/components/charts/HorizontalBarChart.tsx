import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { FacetBucket } from '../../types'
import { useChartPalette } from './palette'

// Single measure (count) across many categories -- one consistent hue, not a
// different color per bar. A per-bar rainbow would redundantly re-encode
// identity the axis labels already carry.
export default function HorizontalBarChart({
  data,
  color,
  labelFor,
}: {
  data: FacetBucket[]
  color: 'bar1' | 'bar2'
  labelFor?: (key: string) => string
}) {
  const palette = useChartPalette()
  const chartData = data.map((d) => ({ ...d, label: labelFor ? labelFor(d.key) : d.key }))
  const height = Math.max(180, chartData.length * 28)

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={chartData} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={palette.gridline} horizontal={false} />
        <XAxis type="number" tick={{ fontSize: 11 }} />
        <YAxis type="category" dataKey="label" tick={{ fontSize: 11 }} width={90} />
        <Tooltip />
        <Bar dataKey="count" fill={palette[color]} radius={[0, 4, 4, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}
