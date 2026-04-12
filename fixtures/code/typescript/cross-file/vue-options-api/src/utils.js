export function formatDate(date) {
  return date.toISOString();
}

export function validate(value) {
  return value != null && value !== '';
}
