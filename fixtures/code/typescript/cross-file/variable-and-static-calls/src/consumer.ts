class Parser {
  parse(input: string): string[] {
    return input.split(",");
  }

  static fromConfig(config: string): Parser {
    return new Parser();
  }
}

export function process(input: string): string[] {
  const p = new Parser();
  return p.parse(input);
}

export function createParser(): Parser {
  return Parser.fromConfig("default");
}

export function runWithService(svc: Parser): string[] {
  return svc.parse("a,b,c");
}
