import api, { api as namedApi, fetchData } from './api';

const reboundApi = api;
const reboundFetch = fetchData;

export function runDefault(): string {
  return api.fetchData();
}

export function runNamed(): string {
  return namedApi.fetchData();
}

export function runAliasObject(): string {
  return reboundApi.fetchData();
}

export function runAliasFunction(): string {
  return reboundFetch();
}
