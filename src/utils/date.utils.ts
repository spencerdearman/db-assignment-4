import { DimDate } from '../models/analytics.models';

/* turns Date object into YYYYMMDD integer key */
export function generateKey(date: Date | null | undefined): number | null {
  if (!date) return null;
  const d = new Date(date);
  const year = d.getFullYear();
  const month = (d.getMonth() + 1).toString().padStart(2, '0');
  const day = d.getDate().toString().padStart(2, '0');
  return parseInt(`${year}${month}${day}`);
}

/* creates a DimDate object */
export function createDimDate(date: Date): DimDate {
  const d = new DimDate();
  const jsDate = new Date(date);

  d.dateKey = generateKey(jsDate) as number;
  d.date = jsDate.toISOString().split('T')[0]!;
  d.year = jsDate.getFullYear();
  d.month = jsDate.getMonth() + 1;
  d.dayOfMonth = jsDate.getDate();
  d.dayOfWeek = jsDate.getDay();
  d.isWeekend = d.dayOfWeek === 0 || d.dayOfWeek === 6;

  const month = d.month;
  if (month <= 3) d.quarter = 1;
  else if (month <= 6) d.quarter = 2;
  else if (month <= 9) d.quarter = 3;
  else d.quarter = 4;

  return d;
}

/* calculates difference between dates */
export function duration(
  startDate: Date | null,
  endDate: Date | null
): number | null {
  if (!startDate || !endDate) return null;
  const start = new Date(startDate);
  const end = new Date(endDate);
  const diffTime = Math.abs(end.getTime() - start.getTime());
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
}
