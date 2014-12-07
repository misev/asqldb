asqldb
======

ASQLDB is an SQL/MDA implementation based on HSQLDB and rasdaman. The SQL/MDA
Part 15 of ISO SQL adds advanced query support for multidimensional arrays.

Getting started
===============
* Configure the rasdaman connection: `editor asqldb.properties`
* If setting up for the first time, please
 * `mkdir -p /var/hsqldb`
 * `chown $USER: /var/hsqldb` (assuming you run hsqldb with `$USER`)
* Go to the build directory: `cd build`, and
 * Install the configuration file to `$HOME/.asqldb.properties` with 
   `ant install-config` if necessary;
 * Build the jar file and start the GUI client: `ant buildrun`

Example
=======
Suppose we have a table in ASQLDB:

    CREATE TABLE Arrays (
      id INTEGER PRIMARY KEY,
      acquired DATE,
      a INTEGER MDARRAY [x]
    );

In `Arrays` we could insert the following data for example, including a sample
1D array of three elements:

    INSERT INTO Arrays
    VALUES (1, '2014-01-22', MDARRAY[x(0:2)] [0,1,2]);

The `MDARRAY` is automatically inserted in rasdaman, while HSQLDB stores its
unique object identifier for reference.
In SELECT queries then we can do advanced array processing on the array column,
and even combine it with other non-array columns, e.g.

    SELECT acquired, avg_cells(a[0:1]) * id
    FROM Arrays

The rasql syntax (http://rasdaman.org/ for more details) is mostly supported as
is, with subtle keyword differences, like `MDARRAY` instead of `MARRAY`, and
`AGGREGATE` instead of `CONDENSE`.

Automated tests
===============

To run the automated JUnit tests:

    cd build
    ant run-asqldb-tests

This will produce summary including number of passed/failed tests in the 
terminal, as well as in ../run-asqldb-tests.txt

Todo
====
* Full DML support (UPDATES missing at the moment)
* Extend documentation with Array SQL specification and examples
* Adapt JDBC driver for multidimensional array results
