asqldb
======

ASQLDB is an Array SQL implementation based on HSQLDB and rasdaman. Array SQL
is an unofficial extension of standard SQL with advanced query support for
multidimensional arrays.

Currently only SELECT queries are supported. An array has to be inserted
directly into rasdaman first, and then published in ASQLDB by inserting it's
OID in a VARCHAR ARRAY column.

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

  INSERT INTO Arrays VALUES (1, '2014-01-22', ARRAY['MYCOLL:100']);

In SELECT queries then we can do advanced array processing on the array column,
and even combine it with other non-array columns. The rasql syntax
(http://rasdaman.org/ for more details) is mostly supported as is.

Todo
====
* Full DML support
* Extend documentation with Array SQL specification and examples
* Adapt JDBC driver for multidimensional array results
