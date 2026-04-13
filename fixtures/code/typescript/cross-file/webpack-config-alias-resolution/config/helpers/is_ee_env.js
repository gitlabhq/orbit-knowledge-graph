const fs = require('fs');
const path = require('path');

const ROOT_PATH = path.resolve(__dirname, '..', '..');

module.exports = fs.existsSync(
  path.join(ROOT_PATH, 'ee', 'app', 'models', 'license.rb'),
);
