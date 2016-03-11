var util = require('util');
var events = require('events');

var winston = require('winston');
var cql = require('cassandra-driver');

var defaultOptions = {
  //column family to store the logs
  table: 'logs',
  //determines if the partition key is changed per day or hour
  partitionBy: 'day',
  consistency: cql.types.consistencies.quorum,
  level: 'info',
  name: 'cassandra'
};

function Cassandra (options) {
  if (!options) {
    throw new Error('Transport options is required');
  }
  if (!options.keyspace) {
    throw new Error('You must specify the options.keyspace');
  }
  this.options = Cassandra.extend({}, defaultOptions, options);
  //winston options
  this.name = this.options.name;
  this.level = this.options.level;
  //create a queue object that will emit the event 'prepared'
  this.schemaStatus = new events.EventEmitter();
  this.schemaStatus.setMaxListeners(0);
  this.client = new cql.Client(this.options);
}

util.inherits(Cassandra, winston.Transport);

Cassandra.prototype.log = function (level, msg, meta, callback) {
  var self = this;
  this._ensureSchema(function (err) {
    if (err) return callback(err, false);
    return self._insertLog(level, msg, meta, function (err) {
      callback(err, !err);
    });
  });
};

/**
 * Gets the log partition key
 */
Cassandra.prototype.getKey = function () {
  if (this.options.partitionBy === 'day') {
    return new Date().toISOString().slice(0, 10);
  }
  else if (this.options.partitionBy === 'hour') {
    return new Date().toISOString().slice(0, 13);
  }
  return null;
};

/**
 * Inserts the log in the db
 */
Cassandra.prototype._insertLog = function (level, msg, meta, callback) {
  var key = this.getKey();
  if (!key) {
    return callback(new Error('Partition ' + this.options.partitionBy + ' not supported'), false);
  }
  //execute as a prepared query as it would be executed multiple times
  return this.client.execute(
    'INSERT INTO ' + this.options.table + ' (key, date, level, message, meta) VALUES (?, ?, ?, ?, ?)',
    [key, new Date(), level, msg, util.inspect(meta)],
    {prepare: true, consistency: this.options.consistency},
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
  var createQuery = 'CREATE TABLE ' + this.options.table +
    ' (key text, date timestamp, level text, message text, meta text, PRIMARY KEY(key, date));';
  var self = this;

  this.client.metadata.getTable(this.options.keyspace, this.options.table, function (err, tableInfo) {
    if (err) return callback(err);
    if (tableInfo) {
      //table is already created
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
module.exports.types = cql.types;
