/**
 * Starmutt
 *
 * An enhanced wrapper for
 * [stardog.js](http://github.com/clarkparsia/stardog.js/)
 */

var crypto = require('crypto');
var events = require('events');
var util = require('util');
var stardog = require('stardog');
var async = require('async');
var jsonld = require('jsonld').promises();

var Promise = require('bluebird');

/**
 * @constructor
 */
function Starmutt() {
  var self = this;
  stardog.Connection.apply(self, arguments);

  var queue = this.queue = async.queue(function(task, callback) {
    self.fetchCache(task, function(err, data) {
      if (data) {
        return callback(null, data, {});
      }

      var method;
      if (task.method === 'getGraph') {
        method = self.getGraphInner;
      }
      else {
        method = stardog.Connection.prototype[task.method];
      }

      method.call(self, task.options, function(body, response) {
          if (body instanceof Error) {
            return callback(body);
          }

          // Stardog.js returns an empty object on an empty result set.
          // Revert to an empty string if the request asked for text.
          if (task.method !== 'getGraph' && task.options.mimetype &&
              task.options.mimetype.substring(0,4) === 'text' &&
              typeof body === 'object') {
            body = '';
          }

          if (response.statusCode !== 200) {
            return callback(new Error(body), new Error(body), response);
          }

          self.putCache(task, body, function() {
            callback(null, body, response);
          });
        });
    });
  });

  queue.concurrency = Infinity;
}
util.inherits(Starmutt, stardog.Connection);

/**
 * Set the cache client to use for caching and enables caching.
 * Caveat: HTTP response data from stardog is not cached.
 * @param {object} cacheClient redis or redis-compatible client.
 * @param {number} ttl         TTL for cache entries.
 */
Starmutt.prototype.setCacheClient = function(cacheClient, ttl) {
  this.cacheClient = cacheClient;
  this.ttl = parseInt(ttl) || 60;
  this.cacheEvents = new events.EventEmitter();
};

/**
 * Get the cache key for a given task.
 * @param  {object} task Query task.
 * @return {string} Cache key based on task.
 */
Starmutt.prototype.cacheKey = function(task) {
  return 'starmutt:' +
    crypto.createHash('sha1')
    .update(JSON.stringify(task)).digest('hex');
};

/**
 * Check whether a task should be cached.
 * @param  {object} task Query task.
 */
Starmutt.prototype.shouldCache = function(task) {
  // Check whether queryOptions.cache === false
  // If so, don't cache
  if (task.options.cache === false) {
    return false;
  }

  // Check whether the query is an INSERT or a DELETE query
  // If it is, don't cache
  var firstTwoChars = task.options.query.trimLeft()
                      .substring(0,2).toLowerCase();
  return !(firstTwoChars === 'in' ||
           firstTwoChars === 'de' ||
           firstTwoChars === 'cl' );
};

/**
 * Fetch query results from cache.
 * @param  {object}   task     Query task.
 * @param  {Function} callback Callback.
 */
Starmutt.prototype.fetchCache = function(task, callback) {
  if (!this.cacheClient || !this.shouldCache(task)) {
    return callback();
  }

  var self = this;
  var cacheKey = this.cacheKey(task);
  this.cacheClient.get(cacheKey, function(err, data) {
    if (!data) {
      self.cacheEvents.emit('miss', cacheKey, task);
      return callback();
    }

    try {
      data = JSON.parse(data);
      self.cacheEvents.emit('hit', cacheKey, task);
      callback(null, data);
    }
    catch (e) {
      callback();
    }
  });
};

/**
 * Put query results to cache.
 * @param  {object}   task     Query task to cache.
 * @param  {object}   results  Query results to cache.
 * @param  {Function} callback Callback.
 */
Starmutt.prototype.putCache = function(task, results, callback) {
  if (!this.cacheClient || !this.shouldCache(task)) {
    return callback();
  }

  var noop = function(err) { return err; };
  var cacheKey = this.cacheKey(task);
  this.cacheClient.set(cacheKey, JSON.stringify(results), noop);
  this.cacheClient.expire(cacheKey, this.ttl, noop);
  this.cacheEvents.emit('put', cacheKey, task, results);
  callback();
};

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
  var self = this;
  if (typeof options === 'string') {
    options = { query: options };
  }
  if (!options.database && self.defaultDatabase) {
    options.database = self.defaultDatabase;
  }

  var promise = new Promise(function(resolve, reject) {
    self.queue.push({ method: method, options: options },
      function(err, body) {
        if (err) {
          return reject(err);
        }

        resolve(body);
      });
  });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    }).catch(callback);
  }

  return promise;
};

/**
 * Query the database. If the 'database' option is not set,
 * will use the value of defaultDatabase.
 * @param  {string|object} The query to execute,
 *                         or options like Connection.query
 * @param  {function} callback
 */
Starmutt.prototype.query = function(options, callback) {
  return this.pushQuery('query', options, callback);
};

/**
 * Query the database for a graph. If the 'database' option is not set,
 * will use the value of defaultDatabase.
 * @param  {object|string} options
 * @param  {Function}      callback
 * @return {void}
 */
Starmutt.prototype.queryGraph = function(options, callback) {
  return this.pushQuery('queryGraph', options, callback);
};

/**
 * Execute a query, accepting callbacks in the (err, data) signature.
 * @param  {object}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
 */
Starmutt.prototype.execQuery = function(queryOptions, callback) {
  return this.pushQuery('query', queryOptions, callback);
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
      return jsonld.compact(doc, context);
      break;
    case 'flattened':
    case 'flatten':
    case 'flat':
      return jsonld.flatten(doc);
      break;
    case 'expanded':
    case 'expand':
      return jsonld.expand(doc);
      break;
    default:
      return Promise.resolve(doc);
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
  // Promises API
  var self = this;
  var promise = this.pushQuery('getGraph', queryOptions)
    .then(function(graph) {
      return processJsonLdOptions(graph, queryOptions);
    });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    }).catch(callback);
  }

  return promise;
};

/**
 * Fetch an NQuads graph from the database and convert to JSON-LD.
 * @param  {[type]}   queryOptions [description]
 * @param  {Function} callback     [description]
 * @return {[type]}                [description]
 */
Starmutt.prototype.getGraphInner = function(queryOptions, callback) {
  if (typeof queryOptions === 'string') {
    queryOptions = { query: queryOptions };
  }
  queryOptions.mimetype = 'text/plain';

  stardog.Connection.prototype.query.call(this,
  queryOptions, function(nquads, resp) {
    if (nquads instanceof Error) {
      return callback(nquads);
    }

    if (resp.statusCode !== 200) {
      return callback(nquads, resp);
    }

    if (typeof nquads === 'object') {
      return callback({}, resp);
    }

    jsonld.fromRDF(nquads, { format: 'application/nquads' })
      .then(function(doc) {
        callback(doc, resp);
      })
      .catch(callback);
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

  var promise = jsonld.normalize(graphToInsert, {format: 'application/nquads'})
    .then(function(normalized) {
      var baseQuery, query;
      if (graphUri) {
        baseQuery = 'INSERT DATA { GRAPH <%s> {\n%s\n} }';
        query = util.format(baseQuery, graphUri, normalized);
      }
      else {
        baseQuery = 'INSERT DATA {\n%s\n}';
        query = util.format(baseQuery, normalized);
      }

      return self.execQuery(query);
    });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    })
    .catch(callback);
  }

  return promise;
};

/**
 * Get resulting bindings from querying the DB.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getResults = function(queryOptions, callback) {
  var promise = this.execQuery(queryOptions).then(function(data) {
    return data.results.bindings;
  });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    })
    .catch(callback);
  }

  return promise;
};

/**
 * Get only the values (discarding datatype and language)
 * from the resulting bindings from querying the DB.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getResultsValues = function(queryOptions, callback) {
  var promise = this.execQuery(queryOptions).then(function(data) {
    var rows = [];
    var bindings = data.results.bindings;
    bindings.forEach(function(binding) {
      var row = {};
      for (var field in binding) {
        row[field] = binding[field].value;
      }
      rows.push(row);
    });

    return rows;
  });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    })
    .catch(callback);
  }

  return promise;
};

/**
 * Get bindings for a single column.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getCol = function(queryOptions, callback) {
  var promise = this.execQuery(queryOptions).then(function(data) {
    var firstCol = data.head.vars[0];
    var col = [];

    var bindings = data.results.bindings;
    bindings.forEach(function(binding) {
      col.push(binding[firstCol]);
    });

    return col;
  });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    })
    .catch(callback);
  }

  return promise;
};

/**
 * Get values for a single column.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getColValues = function(queryOptions, callback) {
  var promise = this.execQuery(queryOptions).then(function(data) {
    var firstCol = data.head.vars[0];
    var colValues = [];

    var bindings = data.results.bindings;
    bindings.forEach(function(binding) {
      colValues.push(binding[firstCol].value);
    });

    return colValues;
  });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    })
    .catch(callback);
  }

  return promise;
};

/**
 * Get a single cell from the resulting bindings.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getVar = function(queryOptions, callback) {
  var promise = this.execQuery(queryOptions).then(function(data) {
    var firstCol = data.head.vars[0];
    return data.results.bindings[0][firstCol];
  });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    })
    .catch(callback);
  }

  return promise;
};

/**
 * Get a single cell's value (discarding datatype and language)
 * from the resulting bindings.
 * @param  {object|string}   queryOptions
 * @param  {Function} callback
 * @return {void}
 */
Starmutt.prototype.getVarValue = function(queryOptions, callback) {
  var promise = this.execQuery(queryOptions).then(function(data) {
    var firstCol = data.head.vars[0];
    return data.results.bindings[0][firstCol].value;
  });

  if (callback) {
    promise.then(function(data) {
      callback(null, data);
    })
    .catch(callback);
  }

  return promise;
};

module.exports = new Starmutt();