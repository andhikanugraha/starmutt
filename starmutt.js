/**
 * Starmutt
 *
 * An enhanced wrapper for
 * [stardog.js](http://github.com/clarkparsia/stardog.js/)
 */

var util = require('util');
var stardog = require('stardog');
var async = require('async');
var jsonld = require('jsonld');
var _ = require('lodash');

/**
 * @constructor
 */
function Starmutt() {
  var self = this;
  stardog.Connection.apply(self, arguments);

  var queue = this.queue = async.queue(function(task, callback) {
    var attempt = function(callback) {
      stardog.Connection.prototype[task.method]
        .call(self, task.options, function(body, response) {
          if (body instanceof Error) {
            console.log('Retrying...', queue.concurrency, queue.delay);

            if (queue.concurrency > 1) {
              --queue.concurrency;
              queue.delay = 2 * queue.delay;
            }

            setTimeout(function() {
              callback(body);
            }, 1000);

            return;
          }

          if (queue.concurrency < queue.maxConcurrency) {
            ++queue.concurrency;
            queue.delay = queue.delay / 2;
          }

          callback(null, [body, response]);
        });
    };

    async.retry(queue.maxRetries, attempt, function(err, result) {
      if (err) {
        return callback(err);
      }

      callback(null, result[0], result[1]);
    });
  });

  queue.delay = 100;
  queue.maxRetries = 8;
  queue.concurrency = queue.maxConcurrency = 8;
}
util.inherits(Starmutt, stardog.Connection);

/**
 * Set default database for executing queries against.
 * @param {string} defaultDatabase The name of the database
 */
Starmutt.prototype.setDefaultDatabase = function(defaultDatabase) {
  this.defaultDatabase = defaultDatabase;
};

/**
 * Get the name of the default database for executing queries against.
 * @return {string} The name of the default database
 */
Starmutt.prototype.getDefaultDatabase = function() {
  return this.defaultDatabase;
};

/**
 * Set the delay between retries.
 * @param {integer} delay
 */
Starmutt.prototype.setDelay = function(delay) {
  this.queue.delay = delay;
};

/**
 * Set the maximum number of retries.
 * @param {integer} maxRetries
 */
Starmutt.prototype.setMaxRetries = function(maxRetries) {
  this.queue.maxRetries = maxRetries;
};

/**
 * Set the level of concurrency.
 * @param {integer} concurrency
 */
Starmutt.prototype.setConcurrency = function(concurrency) {
  this.queue.concurrency = concurrency;
  this.queue.maxConcurrency = concurrency;
};

/**
 * Push a query to the query queue.
 * @param  {string}   method   Either `query` or `queryGraph`.
 * @param  {object}   options  Query options.
 * @param  {Function} callback Callback upon completion.
 * @return {void}
 */
Starmutt.prototype.pushQuery = function(method, options, callback) {
  this.queue.push({ method: method, context: this, options: options },
    function(err, body, response) {
      if (err) {
        return callback(err);
      }
      callback(body, response);
    });
};

/**
 * Query the database. If the 'database' option is not set,
 * will use the value of defaultDatabase.
 * @param  {string|object} The query to execute,
 *                         or options like Connection.query
 * @param  {function} callback
 */
Starmutt.prototype.query = function(options, callback) {
  if (typeof options === 'string') {
    options = { query: options };
  }

  if (this.defaultDatabase) {
    options = _.defaults(options, { database: this.defaultDatabase });
  }

  this.pushQuery('query', options, callback);
};

/**
 * Query the database for a graph. If the 'database' option is not set,
 * will use the value of defaultDatabase.
 * @param  {object|string} options
 * @param  {Function}      callback
 * @return {void}
 */
Starmutt.prototype.queryGraph = function(options, callback) {
  if (typeof options === 'string') {
    options = { query: options };
  }

  if (this.defaultDatabase) {
    options = _.defaults(options, { database: this.defaultDatabase });
  }

  this.pushQuery('queryGraph', options, callback);
};

/**
 * Execute a query, accepting callbacks in the (err, data) signature.
 * @param  {object}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
 */
Starmutt.prototype.execQuery = function(queryOptions, callback) {
  this.query(queryOptions, function(body, response) {
    if (response.statusCode != 200) {
      return callback(body);
    }

    callback(null, body);
  });
};

/**
 * Process JSON-LD options for `getGraph`
 * @param  {object}   doc
 * @param  {object}   options
 * @param  {Function} callback
 * @return {void}
 */
function processJsonLdOptions(doc, options, callback) {
  var context = options.context || {};

  switch (options.form) {
    case 'compact':
      jsonld.compact(doc, context, callback);
      break;
    case 'flattened':
    case 'flatten':
    case 'flat':
      jsonld.flatten(doc, callback);
      break;
    case 'expanded':
    case 'expand':
      jsonld.expand(doc, callback);
      break;
    default:
      callback(null, doc);
  }
}

/**
 * Fetch a JSON-LD graph from the database,
 * with options for formatting the graph output
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getGraph = function(queryOptions, callback) {
  queryOptions.mimetype = 'application/ld+json';

  this.queryGraph(queryOptions, function(data) {
    if (data instanceof Error) {
      // An error
      return callback(data);
    }

    processJsonLdOptions(data, queryOptions, callback);
  });
};

/**
 * Insert a graph into the database.
 * @param  {object}   graphToInsert
 * @param  {string}   graphUri
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.insertGraph = function(graphToInsert, graphUri, callback) {
  if (!callback) {
    callback = graphUri;
    graphUri = null;
  }

  var self = this;

  jsonld.normalize(graphToInsert, {format: 'application/nquads'},
    function(err, normalized) {
      var baseQuery, query;
      if (graphUri) {
        baseQuery = 'INSERT DATA { GRAPH <%s> {\n%s\n} }';
        query = util.format(baseQuery, graphUri, normalized);
      }
      else {
        baseQuery = 'INSERT DATA {\n%s\n}';
        query = util.format(baseQuery, normalized);
      }

      self.execQuery(query, callback);
    });
};

/**
 * Get resulting bindings from querying the DB.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getResults = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (data instanceof Error) {
      // An error
      return callback(data);
    }

    return callback(null, data.results.bindings);
  });
};

/**
 * Get only the values (discarding datatype and language)
 * from the resulting bindings from querying the DB.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getResultsValues = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (data instanceof Error) {
      // An error
      return callback(data);
    }

    var rows = [];
    var bindings = data.results.bindings;
    bindings.forEach(function(binding) {
      var row = {};
      for (var field in binding) {
        row[field] = binding[field].value;
      }
      rows.push(row);
    });

    return callback(null, rows);
  });
};

/**
 * Get bindings for a single column.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getCol = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (data instanceof Error) {
      // An error
      return callback(data);
    }

    var firstCol = data.head.vars[0];
    var col = [];

    var bindings = data.results.bindings;
    bindings.forEach(function(binding) {
      col.push(binding[firstCol]);
    });

    return callback(null, col);
  });
};

/**
 * Get values for a single column.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getColValues = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (data instanceof Error) {
      // An error
      return callback(data);
    }

    var firstCol = data.head.vars[0];
    var colValues = [];

    var bindings = data.results.bindings;
    bindings.forEach(function(binding) {
      colValues.push(binding[firstCol].value);
    });

    return callback(null, colValues);
  });
};

/**
 * Get a single cell from the resulting bindings.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getVar = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (data instanceof Error) {
      // An error
      return callback(data);
    }

    var firstCol = data.head.vars[0];
    return callback(null, data.results.bindings[0][firstCol]);
  });
};

/**
 * Get a single cell's value (discarding datatype and language)
 * from the resulting bindings.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getVarValue = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (data instanceof Error) {
      // An error
      return callback(data);
    }

    var firstCol = data.head.vars[0];
    return callback(null, data.results.bindings[0][firstCol].value);
  });
};

module.exports = new Starmutt();