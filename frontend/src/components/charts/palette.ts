// Mirrors the --chart-*/--quad-*/--map-* custom properties in index.css.
// Duplicated (not read via getComputedStyle) so recharts/leaflet fill/stroke
// props get plain hex reliably -- keep both in sync if either changes.
import { useEffect, useState } from 'react'

const LIGHT = {
  gdelt: '#2a78d6',
  rss: '#eb6834',
  quadPie: ['#2a78d6', '#eb6834', '#1baf7a', '#eda100'],
  mapCooperation: '#2a78d6',
  mapConflict: '#e34948',
  bar1: '#2a78d6',
  bar2: '#1baf7a',
  line: '#2a78d6',
  gridline: '#e1e0d9',
  baseline: '#c3c2b7',
}

const DARK = {
  gdelt: '#3987e5',
  rss: '#d95926',
  quadPie: ['#3987e5', '#d95926', '#199e70', '#c98500'],
  mapCooperation: '#3987e5',
  mapConflict: '#e66767',
  bar1: '#3987e5',
  bar2: '#199e70',
  line: '#3987e5',
  gridline: '#2c2c2a',
  baseline: '#383835',
}

export type ChartPalette = typeof LIGHT

export function usePrefersDark(): boolean {
  const [dark, setDark] = useState(() => window.matchMedia('(prefers-color-scheme: dark)').matches)
  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    const handler = (e: MediaQueryListEvent) => setDark(e.matches)
    mq.addEventListener('change', handler)
    return () => mq.removeEventListener('change', handler)
  }, [])
  return dark
}

export function useChartPalette(): ChartPalette {
  return usePrefersDark() ? DARK : LIGHT
}

// quad_class 1/2 = cooperation, 3/4 = conflict (CAMEO's own structure).
export function quadClassMapColor(quadClass: number | null | undefined, palette: ChartPalette): string {
  if (quadClass == null) return palette.baseline
  return quadClass <= 2 ? palette.mapCooperation : palette.mapConflict
}

export const QUAD_CLASS_LABELS: Record<string, string> = {
  '1': 'Verbal Cooperation',
  '2': 'Material Cooperation',
  '3': 'Verbal Conflict',
  '4': 'Material Conflict',
}
