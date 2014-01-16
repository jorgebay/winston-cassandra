var util = require('util');
var events = require('events');

var winston = require('winston');
var cql = require('node-cassandra-cql');

var defaultOptions = {
  //column family to store the logs
  table: 'logs',
  //determines if the partition key is changed per day or hour
  partitionBy: 'day',
  consistency: cql.types.consistencies.quorum
};

function Cassandra (options) {
  this.name = 'cassandra';
  if (!options) {
    throw new Error('Transport options is required');
  }
  if (!options.keyspace) {
    throw new Error('You must specify the options.keyspace');
  }
  this.options = Cassandra.extend({}, defaultOptions, options);
  //create a queue object that will emit the event 'prepared'
  this.schemaStatus = new events.EventEmitter();
  this.schemaStatus.setMaxListeners(0);
  this.client = new cql.Client(this.options);
}

util.inherits(Cassandra, winston.Transport);

Cassandra.prototype.log = function (level, msg, meta, callback) {
  this._ensureSchema(function (err) {
    if (err) return callback(err, false);
    return this._insertLog(level, msg, meta, function (err) {
      callback(err, !err);
    });
  });
};

/**
 * Inserts the log in the db
 */
Cassandra.prototype._insertLog = function (level, msg, meta, callback) {
  var key = null;
  if (this.options.partitionBy === 'day') {
    key = new Date().toISOString().slice(0, 10);
  }
  else if (this.options.partitionBy === 'hour') {
    key = new Date().toISOString().slice(0, 13);
  }
  else {
    return callback(new Error('Partition not supported'), false);
  }
  return this.client.executeAsPrepared(
    'INSERT INTO ' + this.options.table + ' (key, date, level, message, meta) VALUES (?, ?, ?, ?, ?)',
    [key, new Date(), level, msg, meta],
    this.options.consistency,
    callback);
};

/**
 * Creates the schema the first time and calls callback(err).
 * If its already being created, it queues until it is created (or fails).
 */
Cassandra.prototype._ensureSchema = function (callback) {
  if (this.schemaStatus.err) return callback(this.schemaStatus.err);
  if (this.schemaStatus.created) {
    return callback(null);
  }
  if (this.schemaStatus.creating) {
    return this.schemaStatus.once('prepared', callback);
  }
  this.schemaStatus.creating = true;
  var self = this;
  return this.client.connect(function (err) {
    if (err) {
      self.schemaStatus.err = err;
      return callback(self.schemaStatus.err);
    }
    return self._createSchema(callback);
  });
};

/**
 * Creates the Cassandra column family (table), if its not already created
 */
Cassandra.prototype._createSchema = function (callback) {
  var query = 'SELECT columnfamily_name FROM schema_columnfamilies WHERE keyspace_name=? AND columnfamily_name=?';
  var params = [this.options.keyspace, this.options.table];
  var createQuery = 'CREATE TABLE ' + this.options.table +
    ' (key text, date timestamp, level text, message text, meta text, PRIMARY KEY(key, date));';
  var self = this;
  this.client.execute(query, params, function (err, result) {
    if (err) return callback(err);
    if (result.rows.length === 1) {
      self.schemaStatus.created = true;
      return callback();
    }
    return self.client.execute(createQuery, function (err) {
      self.schemaStatus.created = !err;
      return callback(err);
    });
  });
};

/**
 * Merge the contents of two or more objects together into the first object.
 * Similar to jQuery.extend
 */
Cassandra.extend = function (target) {
  var sources = [].slice.call(arguments, 1);
  sources.forEach(function (source) {
    for (var prop in source) {
      if (source.hasOwnProperty(prop)) {
        target[prop] = source[prop];
      }
    }
  });
  return target;
};

//Define as a property of winston transports for backward compatibility
winston.transports.Cassandra = Cassandra;
module.exports = Cassandra;
//The rest of winston transports uses (module).name convention
//Create a field to allow consumers to interact in the same way
module.exports.Cassandra = Cassandra;