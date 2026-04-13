const fs = require('fs');
const path = require('path');

const ROOT_PATH = path.resolve(__dirname, '..', '..');
const isFossOnly = JSON.parse(process.env.FOSS_ONLY || 'false');

module.exports =
  fs.existsSync(path.join(ROOT_PATH, 'ee', 'app', 'models', 'license.rb')) && !isFossOnly;
