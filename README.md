# starmutt

Starmutt is an enhanced wrapper for [stardog.js](http://github.com/clarkparsia/stardog.js). The idea is to have a single point of reference for an app's `stardog.Connection`, as well have additional utility features on top of it.

This module exposes an instance of a Starmutt class, which extends the `stardog.Connection` class.

### Features

* **Augmented `query()`:**
  * The first parameter can either be a query (string) or the options param as in stardog.js.
  * The first parameter accepts a `reasoning` option, which would set the reasoning mode for the current query only and revert to the previous reasoning mode afterwards.
  * The `database` option is now optional, if a default database is set using `setDefaultDatabase`.
* **Default database:** A default database for querying can be set using `setDefaultDatabase`.
* **`getResults()`, `getCol()`, `getVar()` utility functions** which mimic `get_results`, `get_col`, and `get_var` from WordPress's WPDB class. These functions accept a callback in the standard `(err, data)` signature.

### Usage

	var conn = require('starmutt');
	conn.setEndpoint('http://yourserver:5820/');
	conn.setCredentials('username', 'password');
	conn.setDefaultDatabase('yourDB');

### License

MIT