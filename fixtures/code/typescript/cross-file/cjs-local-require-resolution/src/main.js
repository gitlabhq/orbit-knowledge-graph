const { ROOT_PATH, IS_EE } = require('./config');
const cfg = require('./config');

function readConfig() {
  return { ROOT_PATH, IS_EE, cfg };
}

module.exports = readConfig;
