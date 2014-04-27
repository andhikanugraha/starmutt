/*
Starmutt

An enhanced wrapper for stardog.js.
*/

var util = require('util');
var stardog = require('stardog');
var async = require('async');
var jsonld = require('jsonld');
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
    var reasoningBefore = stardog.Connection.prototype.getReasoning.call(this);
    stardog.Connection.prototype.setReasoning.call(this, options.reasoning);
    var self = this;
    return stardog.Connection.prototype.query.call(this, options, function() {
      stardog.Connection.prototype.setReasoning.call(self, reasoningBefore);
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
    var reasoningBefore = stardog.Connection.prototype.getReasoning.call(this);
    stardog.Connection.prototype.setReasoning.call(this, options.reasoning);
    var self = this;
    return stardog.Connection.prototype.queryGraph.call(this, options, function() {
      console.log("Reasoning before: %j", reasoningBefore);
      stardog.Connection.prototype.setReasoning.call(self, reasoningBefore);
      callback.apply(undefined, arguments);
    });
  }
  else {
    return stardog.Connection.prototype.queryGraph.call(this, options, callback);
  }
}

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
conn.getGraph = function(queryOptions, callback) {
  queryOptions.mimetype = "application/ld+json";

  this.queryGraph(queryOptions, function(data) {
    if (typeof data === 'string') {
      // An error
      return callback(data);
    }

    if (data instanceof Array) {
      async.map(data, function(element, iterationCallback) {
        if (element.rawJSON instanceof Function) {
          iterationCallback(null, element.rawJSON())
        }
        else {
          iterationCallback(element);
        }
      }, function(err, results) {
        processJsonLdOptions(results, queryOptions, callback);
      });
    }
    else if (data.rawJSON instanceof Function) {
      processJsonLdOptions(data.rawJSON(), queryOptions, callback);
    }
    else {
      processJsonLdOptions(data, queryOptions, callback);
    }
  });
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

conn.getResultsValues = function(queryOptions, callback) {
  this.query(queryOptions, function(data) {
    if (typeof data === 'string') {
      // An error
      return callback(data);
    }

    var rows = [];
    var bindings = data.results.bindings;
    console.log(bindings);
    bindings.forEach(function(binding) {
      var row = {};
      for (var field in binding) {
        row[field] = binding[field].value;
      }
      rows.push(row);
    });

    return callback(null, rows);
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