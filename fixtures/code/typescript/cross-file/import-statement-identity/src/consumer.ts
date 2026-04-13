import {
  dateFormats,
  FLOW_METRICS as metrics,
  formatPrecision,
} from "./constants";

export function run() {
  return [dateFormats.iso, metrics.length, formatPrecision()];
}
