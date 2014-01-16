var assert = require('assert');
//Node.js driver client class
var CassandraClient = require('node-cassandra-cql').Client;
//Transport class
var Cassandra = require('../index.js');
var config = require('./config.js');
var extend = Cassandra.extend;
var cqlClient;

before(function (done) {
  this.timeout(5000);
  cqlClient = new CassandraClient(extend({}, config, {hosts: [config.host, config.host2], keyspace: null}));
  cqlClient.connect(done);
});

describe('Cassandra transport', function () {
  this.timeout(5000);
  var cassandra = null;
  var options = extend({hosts: [config.host, config.host2]}, config);

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

    it('should fail if no hosts specified', function () {
      assert.throws(function () {
        new Cassandra({keyspace: 'dummy'});
      });
    });
  });
  describe('#log', function () {
    var queryGetLogs;

    before(function (done) {
      //drop and re create the keyspace
      cqlClient.execute('DROP KEYSPACE ' + config.keyspace, function (err) {
        //ignore the error: the keyspace could not exist
        var query =
          "CREATE KEYSPACE " +
          config.keyspace  +
          " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
        cqlClient.execute(query, done);
      });
      queryGetLogs = 'SELECT * FROM ' + config.keyspace + '.' + cassandra.options.table + ' WHERE key = ? LIMIT 10';
    });

    it('should fail if the keyspace does not exists', function (done) {
      var temp = new Cassandra(extend({}, options, {'keyspace': 'logging_123456'}));
      temp.log('info', 'message', 'metadata', function (err) {
        assert.ok(err, 'It should yield an error');
        done();
      });
    });

    it('should create the table on the keyspace and log', function (done) {
      var logMessage = 'Inserting first message';
      cassandra.log('info', logMessage, 'meta 1', function (err) {
        assert.ok(!err, err);
        cqlClient.execute(queryGetLogs, [cassandra.getKey()], config.consistency, function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows.length, 1, 'Expected 1 row');
          assert.strictEqual(result.rows[0].message, logMessage);
          done();
        });
      });
    });

    it('should not recreate the table if exists', function (done) {
      cassandra.log('info', 'Second message', 'meta 2', function (err) {
        assert.ok(!err, err);
        cqlClient.execute(queryGetLogs, [cassandra.getKey()], config.consistency, function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows.length, 2, 'Expected 2 rows, the table should not be recreated.');
          done();
        });
      });
    });
  });
});