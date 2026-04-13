import { formatValue, toolkit } from './barrel';

const reboundFormat = formatValue;

export function runFormat(value: string): string {
  return reboundFormat(value);
}

export function runToolkit(value: string): string {
  return toolkit.normalize(value);
}
