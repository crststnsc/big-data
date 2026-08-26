// CAMEO taxonomy root event codes (01-20), per the GDELT Event Codebook.
export const EVENT_ROOT_CODE_LABELS: Record<string, string> = {
  '01': 'Make statement',
  '02': 'Appeal',
  '03': 'Intend to cooperate',
  '04': 'Consult',
  '05': 'Diplomatic cooperation',
  '06': 'Material cooperation',
  '07': 'Provide aid',
  '08': 'Yield',
  '09': 'Investigate',
  '10': 'Demand',
  '11': 'Disapprove',
  '12': 'Reject',
  '13': 'Threaten',
  '14': 'Protest',
  '15': 'Military posture',
  '16': 'Reduce relations',
  '17': 'Coerce',
  '18': 'Assault',
  '19': 'Fight',
  '20': 'Mass violence',
}

export function eventRootCodeLabel(code: string): string {
  return EVENT_ROOT_CODE_LABELS[code] ?? code
}
