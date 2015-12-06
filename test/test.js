var assert = require('assert');
// Node.js driver client class
var CassandraClient = require('cassandra-driver').Client;
var winston = require('winston');
var async = require('async');
// Transport class
var Cassandra = require('../index.js');
var config = require('./config.js');
var extend = Cassandra.extend;
var cqlClient;

before(function (done) {
  cqlClient = new CassandraClient(extend({}, config, {keyspace: null}));
  cqlClient.connect(done);
});

describe('Cassandra transport', function () {
  var cassandra = null;
  var options = extend({}, config);

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
        new Cassandra({contactPoints: config.contactPoints});
      });
    });

    it('should fail if no contactPoints specified', function () {
      assert.throws(function () {
        new Cassandra({keyspace: 'dummy'});
      });
    });
  });
  describe('#log', function () {
    var queryGetLogs;

    before(function (done) {
      // drop and re create the keyspace
      cqlClient.execute('DROP KEYSPACE ' + config.keyspace, function () {
        // ignore the error: the keyspace could not exist
        var query =
          "CREATE KEYSPACE " +
          config.keyspace +
          " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
        cqlClient.execute(query, done);
      });
      queryGetLogs = 'SELECT * FROM ' + config.keyspace + '.' + cassandra.options.table + ' WHERE key = ? LIMIT 10';
    });

    it('should fail if the keyspace does not exists', function (done) {
      var temp = new Cassandra(extend({}, options, {keyspace: 'logging_123456'}));
      temp.log('info', 'message', 'metadata', function (err) {
        assert.ok(err, 'It should yield an error');
        done();
      });
    });

    it('should create the table on the keyspace and log', function (done) {
      var logMessage = 'Inserting first message';
      cassandra.log('info', logMessage, 'meta 1', function (err) {
        assert.ok(!err, err);
        cqlClient.execute(queryGetLogs, [cassandra.getKey()], {consistency: config.consistency}, function (err, result) {
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
        cqlClient.execute(queryGetLogs, [cassandra.getKey()], {consistency: config.consistency}, function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows.length, 2, 'Expected 2 rows, the table should not be recreated.');
          done();
        });
      });
    });

    it('should insert log from winston', function (done) {
      assert.ok(winston.transports.Cassandra, 'Cassandra should be defined as transport when required');
      var transport = new winston.transports.Cassandra(options);
      var logger = new (winston.Logger)({
        transports: [transport]
      });
      async.series([
        function (next) {
          logger.log('info', 'Through winston without meta', next);
        },
        function (next) {
          logger.log('info', 'Through winston with meta', {val: 1}, next);
        }
      ], function (err) {
        assert.ifError(err);
        cqlClient.execute(queryGetLogs, [cassandra.getKey()], {consistency: config.consistency}, function (err, result) {
          assert.ok(!err, err);
          assert.strictEqual(result.rows.length, 4, 'Expected 4 rows, total inserted so far');
          done(err);
        });
      });
    });
  });
});
