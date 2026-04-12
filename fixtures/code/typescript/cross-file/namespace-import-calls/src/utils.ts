export function normalize(value: string): string {
  return value.trim().toLowerCase();
}

export function validate(value: string): boolean {
  return value.length > 0;
}

export class Parser {
  parse(input: string): string[] {
    return input.split(",");
  }
}
