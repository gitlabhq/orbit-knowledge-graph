const path = require('path');

const ROOT_PATH = path.resolve(__dirname, '..');
const IS_EE = require('./helpers/is_ee_env');

module.exports = {
  ROOT_PATH,
  IS_EE,
};
