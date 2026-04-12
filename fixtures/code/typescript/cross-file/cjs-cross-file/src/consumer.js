var utils = require('./utils');

function process(input) {
  if (utils.validate(input)) {
    return utils.normalize(input);
  }
  return input;
}

module.exports = process;
