asqldb
======

ASQLDB is an SQL/MDA implementation based on HSQLDB and rasdaman. The SQL/MDA
Part 15 of ISO SQL adds advanced query support for multidimensional arrays.

Currently only SELECT queries are supported. An array has to be inserted
directly into rasdaman first, and then published in ASQLDB by inserting it's
OID in a VARCHAR ARRAY column.

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
      array VARCHAR(20) ARRAY
    );

To publish an array object in collection MYCOLL, with OID = 100, then in Arrays
we would insert the following:

    INSERT INTO Arrays
    VALUES (1, '2014-01-22', ARRAY['MYCOLL:100']);

In SELECT queries then we can do advanced array processing on the array column,
and even combine it with other non-array columns, e.g.

    SELECT acquired, avg_cells(array[0:100, 50:200]) * id
    FROM Arrays

The rasql syntax (http://rasdaman.org/ for more details) is mostly supported as
is, with subtle keyword differences, like `ARRAY` instead of `MARRAY`, and
`AGGREGATE` instead of `CONDENSE`.

Automated tests
===============
To run ASQLDB specific tests

    cd build
    ant testselect
    
Currently around 30% of the tests are failing, SELECT is not fully supported yet.

Todo
====
* Full DML support
* Extend documentation with Array SQL specification and examples
* Adapt JDBC driver for multidimensional array results
