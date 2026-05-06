'use strict';

// node-gyp-build looks for a prebuild matching process.platform +
// process.arch first; if none exists, it falls back to a from-source
// build via node-gyp using binding.gyp. This is what lets the same
// package work on every supported platform without users needing a
// C toolchain when a prebuild is shipped.
const binding = require('node-gyp-build')(__dirname + '/..');

const { Database, Statement, version } = binding;

module.exports = Database;
module.exports.Database = Database;
module.exports.Statement = Statement;
module.exports.version = version;
