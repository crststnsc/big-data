import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { DateBucket } from '../../types'
import { useChartPalette } from './palette'

export default function VolumeChart({ data }: { data: DateBucket[] }) {
  const palette = useChartPalette()

  return (
    <ResponsiveContainer width="100%" height={280}>
      <AreaChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={palette.gridline} vertical={false} />
        <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={40} />
        <YAxis tick={{ fontSize: 11 }} />
        <Tooltip />
        <Legend />
        <Area
          type="monotone"
          dataKey="gdelt_event"
          name="GDELT events"
          stackId="1"
          stroke={palette.gdelt}
          fill={palette.gdelt}
          fillOpacity={0.5}
        />
        <Area
          type="monotone"
          dataKey="rss_article"
          name="RSS articles"
          stackId="1"
          stroke={palette.rss}
          fill={palette.rss}
          fillOpacity={0.5}
        />
      </AreaChart>
    </ResponsiveContainer>
  )
}
