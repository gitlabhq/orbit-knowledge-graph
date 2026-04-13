import { normalize } from 'ee_else_ce/utils';

export function run(value: string): string {
  return normalize(value);
}
