asqldb
======

ASQLDB is an SQL/MDA implementation based on HSQLDB and rasdaman. The SQL/MDA
Part 15 of ISO SQL adds advanced query support for multidimensional arrays.

Getting started
===============
* Configure the rasdaman connection: `editor config.properties`
* If setting up for the first time, please
 * `mkdir -p /var/hsqldb`
 * `chown $USER: /var/hsqldb` (assuming you run hsqldb with `$USER`)
* Go to the build directory: `cd build`, and
 * Build the jar file: `ant hsqldb`
 * Run the GUI tool: `ant run`

Example
=======
Suppose we have a table in ASQLDB:

    CREATE TABLE Arrays (
      id INTEGER PRIMARY KEY,
      acquired DATE,
      a INTEGER MDARRAY [x]
    );

To publish an array object in collection MYCOLL, with OID = 100, then in Arrays
we would insert the following for example:

    INSERT INTO Arrays
    VALUES (1, '2014-01-22', MDARRAY[x(0:2)] [0,1,2]);

In SELECT queries then we can do advanced array processing on the array column,
and even combine it with other non-array columns, e.g.

    SELECT acquired, avg_cells(a[0:1]) * id
    FROM Arrays

The rasql syntax (http://rasdaman.org/ for more details) is mostly supported as
is, with subtle keyword differences, like `MDARRAY` instead of `MARRAY`, and
`AGGREGATE` instead of `CONDENSE`.

Automated tests
===============


Todo
====
* Full DML support (UPDATES missing at the moment)
* Extend documentation with Array SQL specification and examples
* Adapt JDBC driver for multidimensional array results
