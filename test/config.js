var fs = require('fs');
var path = require('path');
var utils = require('../../node-cassandra-cql/src-master/lib/utils.js');

var config = {
  "host": "localhost",
  "host2": "localhost",
  "port": 9042,
  "username": "cassandra",
  "password": "cassandra",
  "keyspace": "logging"
};

if (fs.existsSync(path.resolve(__dirname, './localConfig.json'))) {
  var localConfig = require('./localConfig.json');
  utils.extend(config, localConfig);
}

module.exports = config;