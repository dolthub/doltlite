'use strict';

// Native binding compiled by node-gyp at install time.
const binding = require('../build/Release/doltlite.node');

const { Database, Statement, version } = binding;

module.exports = Database;
module.exports.Database = Database;
module.exports.Statement = Statement;
module.exports.version = version;
