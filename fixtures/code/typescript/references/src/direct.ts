export function helper(value: string): string {
  return value.toUpperCase();
}

export function normalize(value: string): string {
  return helper(value).trim();
}
