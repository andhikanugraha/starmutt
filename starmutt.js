/*
Starmutt

An enhanced wrapper for stardog.js.
*/

var util = require('util');
var stardog = require('stardog');
var _ = require('underscore');

function Starmutt() {
  stardog.Connection.apply(this, arguments);
}
util.inherits(Starmutt, stardog.Connection);

var conn = Starmutt.prototype;

conn.setDefaultDatabase = function(defaultDatabase) {
  this.defaultDatabase = defaultDatabase;
}

conn.getDefaultDatabase = function() {
  return this.defaultDatabase;
}

// Augment Connection.query with:
// * Specifying a query instead of options object as first param
// * Default database param
// * Specifying reasoning for the scope of one query only
conn.query = function(options, callback) {
  if (typeof options === 'string') {
    options = { query: options }
  }

  if (this.defaultDatabase) {
    options = _.extend({ database: this.defaultDatabase }, options);
  }

  if (options.reasoning) {
    var reasoningBefore = stardog.Connection.prototype.getReasoning();
    stardog.Connection.prototype.setReasoning.call(this, options.reasoning);
    return stardog.Connection.prototype.query.call(this, options, function() {
      stardog.Connection.prototype.setReasoning.call(this, reasoningBefore);
      callback.apply(undefined, arguments);
    });
  }
  else {
    return stardog.Connection.prototype.query.call(this, options, callback);
  }
}

conn.queryGraph = function(options, callback) {
  if (typeof options === 'string') {
    options = { query: options }
  }

  if (this.defaultDatabase) {
    options = _.extend({ database: this.defaultDatabase }, options);
  }

  if (options.reasoning) {
    var reasoningBefore = stardog.Connection.prototype.getReasoning();
    stardog.Connection.prototype.setReasoning.call(this, options.reasoning);
    return stardog.Connection.prototype.queryGraph.call(this, options, function() {
      stardog.Connection.prototype.setReasoning.call(this, reasoningBefore);
      callback.apply(undefined, arguments);
    });
  }
  else {
    return stardog.Connection.prototype.queryGraph.call(this, options, callback);
  }
}

conn.getResults = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (typeof data === 'string') {
      // An error
      return callback(data);
    }

    return callback(null, data.results.bindings);
  });
}

conn.getCol = function(queryOptions, callback) {
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
}

conn.getColValues = function(queryOptions, callback) {
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
}

conn.getVar = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (typeof data === 'string') {
      // An error
      return callback(data);
    }

    var firstCol = data.head.vars[0];
    return callback(null, data.results.bindings[0][firstCol]);
  });
}

conn.getVarValue = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (typeof data === 'string') {
      // An error
      return callback(data);
    }

    var firstCol = data.head.vars[0];
    return callback(null, data.results.bindings[0][firstCol].value);
  });
}

module.exports = new Starmutt();