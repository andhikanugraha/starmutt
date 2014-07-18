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
  stardog.Connection.apply(this, arguments);
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

  return stardog.Connection.prototype.query.call(this, options, callback);
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

  return stardog.Connection.prototype.queryGraph.call(this, options, callback);
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
    if (typeof data === 'string') {
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
    if (typeof data === 'string') {
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
    if (typeof data === 'string') {
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
    if (typeof data === 'string') {
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
    if (typeof data === 'string') {
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
    if (typeof data === 'string') {
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
    if (typeof data === 'string') {
      // An error
      return callback(data);
    }

    var firstCol = data.head.vars[0];
    return callback(null, data.results.bindings[0][firstCol].value);
  });
};

module.exports = new Starmutt();