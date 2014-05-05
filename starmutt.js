/**
 * Starmutt
 *
 * An enhanced wrapper for [stardog.js](http://github.com/clarkparsia/stardog.js/)
 */

var util = require('util');
var stardog = require('stardog');
var async = require('async');
var jsonld = require('jsonld');
var _ = require('underscore');

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
 * Query the database. If the 'database' option is not set, will use the value of defaultDatabase.
 * @param  {string|object} The query to execute, or options like Connection.query
 * @param  {function} callback
 */
Starmutt.prototype.query = function(options, callback) {
  if (typeof options === 'string') {
    options = { query: options };
  }

  if (this.defaultDatabase) {
    options = _.extend({ database: this.defaultDatabase }, options);
  }

  return stardog.Connection.prototype.query.call(this, options, callback);
};

/**
 * [queryGraph description]
 * @param  {[type]}   options
 * @param  {Function} callback
 * @return {[type]}
 */
Starmutt.prototype.queryGraph = function(options, callback) {
  if (typeof options === 'string') {
    options = { query: options };
  }

  if (this.defaultDatabase) {
    options = _.extend({ database: this.defaultDatabase }, options);
  }

  return stardog.Connection.prototype.queryGraph.call(this, options, callback);
};

/**
 * [execQuery description]
 * @param  {[type]}   queryOptions
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

function processJsonLdOptions(doc, options, callback) {
  var context = options.context || {};

  switch (options.form) {
    case 'compact':
      jsonld.compact(doc, context, callback);
      break;
    case 'flattened':
    case 'flatten':
    case 'flat':
      jsonld.flatten(newDoc, callback);
      break;
    case 'expanded':
    case 'expand':
      jsonld.expand(doc, callback);
      break;
    default:
      callback(null, doc);
  }
}

// stardog.js's queryGraph is tacky, so just return the raw JSON-LD.
/**
 * [getGraph description]
 * @param  {[type]}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
 */
Starmutt.prototype.getGraph = function(queryOptions, callback) {
  queryOptions.mimetype = "application/ld+json";

  this.queryGraph(queryOptions, function(data) {
    if (typeof data === 'string') {
      // An error
      return callback(data);
    }

    console.log(data);

    processJsonLdOptions(data, queryOptions, callback);
  });
};

/**
 * [insertGraph description]
 * @param  {[type]}   graphToInsert
 * @param  {[type]}   graphUri
 * @param  {Function} callback
 * @return {[type]}
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
 * [getResults description]
 * @param  {[type]}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
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
 * [getResultsValues description]
 * @param  {[type]}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
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
 * [getCol description]
 * @param  {[type]}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
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
 * [getColValues description]
 * @param  {[type]}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
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
 * [getVar description]
 * @param  {[type]}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
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
 * [getVarValue description]
 * @param  {[type]}   queryOptions
 * @param  {Function} callback
 * @return {[type]}
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