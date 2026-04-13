const path = require('path');
const { ROOT_PATH, IS_EE } = require('./webpack.constants');

const alias = {
  ee_else_ce: path.join(ROOT_PATH, 'app/assets/javascripts'),
};

if (IS_EE) {
  Object.assign(alias, {
    ee_else_ce: path.join(ROOT_PATH, 'ee/app/assets/javascripts'),
  });
}

module.exports = {
  resolve: {
    alias,
  },
};
