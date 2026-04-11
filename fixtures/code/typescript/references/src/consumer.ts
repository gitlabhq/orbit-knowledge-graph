import { normalize } from "./reexports";
import defaultFormat from "./default_formatter";

export function run(value: string): string {
  return defaultFormat(normalize(value));
}
