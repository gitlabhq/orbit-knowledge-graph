import * as utils from "./utils";

export function process(input: string): string {
  const valid = utils.validate(input);
  if (valid) {
    return utils.normalize(input);
  }
  return input;
}

export function createParser(): utils.Parser {
  return new utils.Parser();
}
