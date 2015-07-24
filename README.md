# winston-cassandra

A Cassandra transport for [winston](https://github.com/flatiron/winston) logging library.

## Installation
``` bash
  $ npm install winston
  $ npm install winston-cassandra
```
[![Build Status](https://secure.travis-ci.org/jorgebay/winston-cassandra.png)](http://travis-ci.org/jorgebay/winston-cassandra)

## Usage
``` js
  var winston = require('winston');

  // Adds a Cassandra transport (it also adds the field `winston.transports.Cassandra`)
  winston.add(require('winston-cassandra'), options);
```

The Cassandra transport accepts the following options:

* __level:__ Level of messages that this transport should log (default: `'info'`).
* __table:__ The name of the Cassandra column family you want to store log messages in (default: `'logs'`).
* __partitionBy:__ How you want the logs to be partitioned. Possible values `'hour'` and `'day'`(Default).
* __consistency:__ The consistency of the insert query (default: `quorum`).
* __name:__ Name of the transport.

In addition to the options accepted by the [Node.js Cassandra driver](https://github.com/datastax/nodejs-driver).

* __contactPoints:__ Cluster nodes that will handle the write requests:
Array of strings containing the hosts, for example `['host1', 'host2']` (required).
* __keyspace:__ The name of the keyspace that will contain the logs table (required). The keyspace should be already created in the cluster.

## License
Distributed under the [MIT license](https://github.com/jorgebay/winston-cassandra/blob/master/LICENSE.txt).
