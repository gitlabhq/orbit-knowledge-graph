import { HTTP_STATUS_OK, formatter } from './constants';

registerStatus(HTTP_STATUS_OK);

export function run(value: string): string {
  return formatter(value);
}

export function attempt() {
  return HTTP_STATUS_OK();
}

function registerStatus(status: number): number {
  return status;
}
