import { normalize } from "./reexports";

export function run(value: string): string {
  return normalize(value);
}
