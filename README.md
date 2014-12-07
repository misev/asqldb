asqldb
======

ASQLDB is an SQL/MDA implementation based on HSQLDB and rasdaman. The SQL/MDA
Part 15 of ISO SQL adds advanced query support for multidimensional arrays.

Getting started
===============
* Go to your ASQLDB sources: `cd $ASQLDB_HOME`

* If setting up for the first time, please run:
 * `mkdir -p /var/hsqldb`
 * `chown $USER: /var/hsqldb` (assuming you run ASQLDB with `$USER`)

* Configure the rasdaman connection:
 * `cp sample/asqldb.properties $HOME/.asqldb.properties`
 * `editor $HOME/.asqldb.properties`

* Go to the build directory, build the jar file and start the GUI client:
 * `cd build`
 * `ant buildrun`

Command-line client
-------------------
SqlTool is a JDBC client that allows executing queries from the command line.

Before running the configurations needs to be set:
 * `cp sample/sqltool.rc $HOME`
 * `editor $HOME/sqltool.rc` (to adapt it)

Build the latest SqlTool as an sqltool.jar in the lib directory:
 * `cd build && ant sqltool`

To run it:
 * `java -jar lib/sqltool.jar`
 * `java -jar lib/sqltool.jar --help` (to get more information)

For example, executing the SQL commands in file setup.sql with the `personal` 
"urlid" from the default `sqltool.rc` could be done with:

    java -jar lib/sqltool.jar personal setup.sql

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
 * `cd build`
 * `ant run-asqldb-tests`

This will produce summary including number of passed/failed tests in the 
terminal, as well as in ../run-asqldb-tests.txt

Todo
====
* Full DML support (UPDATES missing at the moment)
* Extend documentation with Array SQL specification and examples
* Adapt JDBC driver for multidimensional array results
