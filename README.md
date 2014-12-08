asqldb
======

ASQLDB is an SQL/MDA implementation based on HSQLDB and rasdaman. The SQL/MDA
Part 15 of ISO SQL adds advanced query support for multidimensional arrays.

Getting started
===============
* Assuming you have downloaded the ASQLDB sources to $ASQLDB_HOME:
 * `cd $ASQLDB_HOME`

* If setting up for the first time, please run:
 * `mkdir -p /var/hsqldb`
 * `chown $USER: /var/hsqldb` (assuming you run ASQLDB with `$USER`)

* Configure the rasdaman connection:
 * `mkdir -p $HOME/.asqldb`
 * `cp sample/rasdaman.properties $HOME/.asqldb/`
 * `editor $HOME/.asqldb/rasdaman.properties` (to adapt it)

* Configure the ASQLDB connection:
 * `mkdir -p $HOME/.asqldb`
 * `cp sample/sqltool.rc $HOME/.asqldb/`
 * `editor $HOME/.asqldb/sqltool.rc` (to adapt it)

GUI client
----------
The Database Manager is a GUI client for ASQLDB. To start, go to the build 
directory and build lib/hsqldb.jar:
 * `cd build && ant hsqldb`

The GUI manager can be started with the `bin/sqlgui` bash script 
(--help for more information). You can link the script to /usr/bin for 
convenience `sudo ln -s $ASQLDB_HOME/bin/sqlgui /usr/bin`, and then use it
simply with `sqlgui`.

The `sqlgui` script is simply a wrapper that executes the hsqldb.jar; you can
manually run this for better control:
 * `java -cp lib/hsqldb.jar:lib/rasj.jar org.hsqldb.util.DatabaseManagerSwing`
(--help for more information)
 * check the Utilities documentation for full documentation on the
Database Manager: `doc/util-guide/index.html`

Command-line client
-------------------
SqlTool is a JDBC client that allows executing queries from the command line.
To start build the latest SqlTool as lib/sqltool.jar:
 * `cd build && ant sqltool`

To run it:
 * `java -jar lib/sqltool.jar`

For example, executing the SQL commands in file setup.sql with the `personal` 
"urlid" from the default `sqltool.rc` could be done with:

    java -jar lib/sqltool.jar personal setup.sql

More information can be found at:
 * `java -jar lib/sqltool.jar --help`
 * the Utilities documentation, `doc/util-guide/index.html`

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
