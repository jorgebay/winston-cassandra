var assert = require('assert');

var Cassandra = require('../index.js');
var config = require('./config.js');
var extend = Cassandra.extend;

describe('Cassandra transport', function () {
  var cassandra = null;
  var options = {
    hosts: [config.host, config.host2],
    username: config.username,
    password: config.password,
    keyspace: config.keyspace
  };

  before(function () {
    cassandra = new Cassandra(options);
  });
  describe('constructor', function () {
    it('should fail if no options specified', function () {
      assert.throws(function () {
        new Cassandra();
      });
    });

    it('should fail if no keyspace specified', function () {
      assert.throws(function () {
        new Cassandra({hosts: [config.host]});
      });
    });
  });
  describe('#log', function () {
    it('should fail if the keyspace does not exists', function (done) {
      var temp = new Cassandra(extend({}, options, {'keyspace': 'logging_123456'}));
      temp.log('info', 'message', 'metadata', function (err) {
        assert.ok(err, 'It should yield an error');
        done();
      });
    });
  });
});