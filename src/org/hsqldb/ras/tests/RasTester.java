package org.hsqldb.ras.tests;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import org.hsqldb.ras.RasUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Johannes on 5/5/14.
 *
 * @author Johannes Bachhuber
 */
public class RasTester {

    public static final String DEFAULT_DB_FILE = "/var/hsqldb/db";

    private String dbFile = DEFAULT_DB_FILE;

    private PrintStream out = System.out;

    private boolean testSql;
    private boolean testRas;


    public static void main(String[] args) throws SQLException, JSAPException {
        JSAP jsap = new JSAP();

        FlaggedOption opt1 = new FlaggedOption("dbFile")
                .setStringParser(JSAP.STRING_PARSER)
                .setDefault(DEFAULT_DB_FILE)
                .setRequired(false)
                .setShortFlag('d')
                .setLongFlag("database-file");

        jsap.registerParameter(opt1);

        Switch sw1 = new Switch("sql")
                .setShortFlag('s')
                .setLongFlag("sql");

        jsap.registerParameter(sw1);

        Switch sw2 = new Switch("noras")
                .setShortFlag(JSAP.NO_SHORTFLAG)
                .setLongFlag("noras");

        jsap.registerParameter(sw2);

        Switch sw3 = new Switch("benchmark")
                .setShortFlag('b')
                .setLongFlag("benchmark");
        jsap.registerParameter(sw3);

        FlaggedOption opt2 = new FlaggedOption("count")
                .setStringParser(JSAP.INTEGER_PARSER)
                .setDefault("10")
                .setRequired(false)
                .setShortFlag('n')
                .setLongFlag("count");

        jsap.registerParameter(opt2);

        UnflaggedOption opt3 = new UnflaggedOption("files")
                .setStringParser(JSAP.STRING_PARSER)
                .setDefault("testrun/asqldb/benchmark.txt")
                .setRequired(false)
                .setGreedy(true);

        jsap.registerParameter(opt3);

        JSAPResult config = jsap.parse(args);

        RasTester rasTester = new RasTester(config);
        if (config.getBoolean("benchmark")) {
            final String[] files = config.getStringArray("files");
            System.out.println("Running benchmarks...");
            for (String file : files) {
                final double[] times = rasTester.benchmark(file, config.getInt("count"));
                System.out.println(String.format(
                        "asql: %f, rasql: %f, without closing: %f - for %s", times[0], times[1], times[2], file));
            }

        } else {
            rasTester.test();
        }

        // check whether the command line was valid, and if it wasn't,
        // display usage information and exit.
        if (!config.success()) {
            System.err.println();
            System.err.println("Usage: java ... ");
            System.err.println("                "
                    + jsap.getUsage());
            System.err.println();
            System.exit(1);
        }
    }

    public RasTester(final JSAPResult config) {

        dbFile = config.getString("dbFile");
        testSql = config.getBoolean("sql");
        testRas = !config.getBoolean("noras");
    }

    public void test() throws SQLException {
        boolean success = true;
        Connection conn = null;
        try {
            conn = getConnection();



            if (testRas) {
                success = setUp(conn);
            }

            //test queries:
            success = success && runTests(conn);
        } catch (final SQLException e) {
            throw new RuntimeException("Tests FAILED. SQLException occurred while performing tests.", e);
        }
        finally {
            if (testRas) {
                tearDown(conn);
            }
        }

        if (success) {
            out.println("All tests PASSED");
        } else {
            out.println("Tests FAILED.");
        }
    }

    public double[] benchmark(final String file, final int n) throws SQLException{

        Connection conn = null;
        double[] executionTime = new double[]{-1, -1, -1};
        try {
            conn = getConnection();
            setUp(conn);

            List<String> queries;
            queries = Files.readAllLines(Paths.get(file), Charset.forName("UTF-8"));

            //run once to "warm up" (load classes, etc) and get queries for direct rasql test
            RasUtil.setQueryOutputStream(new PrintStream(new File(file+".rasql")));
            benchmarkBlock(conn, queries);
            RasUtil.setQueryOutputStream(System.out);

            RasUtil.printLog = false;
            long startTime = System.nanoTime();
            //start benchmark
            for (int i=0; i < n; ++i) {
                benchmarkBlock(conn, queries);
            }
            //end benchmark
            long endTime = System.nanoTime();
            executionTime[0] = (endTime - startTime)/1000000000.0/n;


            //direct rasql queries:
            queries = Files.readAllLines(Paths.get(file+".rasql"), Charset.forName("UTF-8"));
            startTime = System.nanoTime();
            //start benchmark
            for (int i=0; i < n; ++i) {
                benchmarkRasqlBlock(queries, true);
            }
            //end benchmark
            RasUtil.printLog = true;
            endTime = System.nanoTime();
            executionTime[1] = (endTime - startTime)/1000000000.0/n;

            //direct rasql queries without closing db in between
            startTime = System.nanoTime();
            //start benchmark
            for (int i=0; i < n; ++i) {
                benchmarkRasqlBlock(queries, false);
            }
            RasUtil.closeDatabase();
            //end benchmark
            RasUtil.printLog = true;
            endTime = System.nanoTime();
            executionTime[2] = (endTime - startTime)/1000000000.0/n;


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException("File "+file+" could not be opened.", e);
        }
        finally {
            tearDown(conn);
        }
        return executionTime;
    }

    private static void benchmarkBlock(final Connection conn, final List<String> queries) throws SQLException {
        for (final String query : queries) {
            if (query.isEmpty() || query.startsWith("-"))
                continue;
            Statement stmt = null;
            try {
                stmt = conn.createStatement();
                stmt.executeQuery(query);
            } finally {
                if (stmt != null) { stmt.close(); }
            }
        }
    }

    private static void benchmarkRasqlBlock(final List<String> queries, final boolean closeWhenDone) throws SQLException {
        for (final String query : queries) {
            if (query.isEmpty() || query.startsWith("-"))
                continue;
            RasUtil.executeRasqlQuery(query, closeWhenDone, false);
        }

    }

    private boolean setUp(final Connection conn) throws SQLException {
        boolean success = dropTables(conn);
        success = success && createTables(conn);
        success = success && insertValues(conn);
        return success;
    }

    private boolean tearDown(final Connection conn) throws SQLException {
        dropRasCollections();
        return conn == null || dropTables(conn);
    }

    public boolean createTables(final Connection conn) throws SQLException {
        String createString =
                "create table RASTEST (ID integer NOT NULL, COLL varchar(40) ARRAY NOT NULL, PRIMARY KEY (ID))";
        String createString2 =
                "create table RASTEST2 (ID integer NOT NULL, COLL varchar(40) ARRAY NOT NULL, COLL2 varchar(40) ARRAY NOT NULL, PRIMARY KEY (ID))";
        return executeQuery(conn, createString, 0) && executeQuery(conn, createString2, 0);
    }

    public boolean insertValues(final Connection conn) throws SQLException {
        RasUtil.openDatabase(RasUtil.adminUsername, RasUtil.adminPassword, true);
        RasUtil.executeRasqlQuery("create collection rastest GreySet",
                false, false);
        RasUtil.executeRasqlQuery("insert into rastest values marray x in [0:250, 0:225] values 0c",
                false, false);
        RasUtil.executeRasqlQuery("create collection rastest2 GreySet",
                false, false);
        RasUtil.executeRasqlQuery("insert into rastest2 values marray x in [0:225, 0:225] values 2c",
                true, false);

        String oidQuery = "select oid(c) from rastest as c";
        String oid = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid = oid.replaceAll("[\\[\\]]", "");
        oidQuery = "select oid(c) from rastest2 as c";
        String oid2 = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid2 = oid2.replaceAll("[\\[\\]]", "");
        String[] insertQueries = new String[]{
                "INSERT INTO RASTEST VALUES(0, ARRAY['rastest:" + Double.valueOf(oid).intValue() + "'])",
                "INSERT INTO RASTEST VALUES(1, ARRAY['rastest2:" + Double.valueOf(oid2).intValue() + "'])",
                "INSERT INTO RASTEST2 VALUES(0, ARRAY['rastest:" + Double.valueOf(oid).intValue() + "'], ARRAY['rastest2:" + Double.valueOf(oid2).intValue() + "'])",
                "INSERT INTO RASTEST2 VALUES(1, ARRAY['rastest2:" + Double.valueOf(oid2).intValue() + "'], ARRAY['rastest:" + Double.valueOf(oid).intValue() + "'])"
        };
        for (String query : insertQueries) {
            if (!executeQuery(conn, query, 0))
                return false;
        }
        return true;
    }

    public boolean runTests(final Connection conn) throws SQLException {
        final String rasqlTests = "testrun/asqldb/mixedQueries.txt";
        final String sqlArithmetic = "testrun/hsqldb/TestSelfArithmetic.txt";
        final String sqlQueries = "testrun/hsqldb/TestSelfQueries.txt";
        final String sqlJoins = "testrun/hsqldb/TestSelfJoins.txt";
        final List<String> testFiles = new ArrayList<String>();
        if (testRas)
            testFiles.add(rasqlTests);
        if (testSql)
            testFiles.addAll(Arrays.asList(sqlArithmetic, sqlQueries, sqlJoins));

        List<String> queries;
        for (String file : testFiles) {
            out.println("\n===========================\n" +
                        " Running queries in "+file+"\n" +
                        "===========================");
            try {
                queries = Files.readAllLines(Paths.get(file), Charset.forName("UTF-8"));
            } catch (IOException e) {
                throw new RuntimeException("File "+file+" could not be opened.", e);
            }
            for (int i = 0; i < queries.size(); i++) {
                String query = queries.get(i);
                if (query.isEmpty() || query.startsWith("-"))
                    continue;
                while(queryHasAnotherLine(query)) {
                    query += queries.get(++i);
                }
                if (!executeQuery(conn, query, i+1))
                    return false;
            }
        }
        return true;
    }

    private boolean queryHasAnotherLine(final String query) {
        switch(query.charAt(query.length()-1)) {
            case '(':
            case ')':
            case ',':
                return true;
        }
        return false;
    }

    public boolean dropTables(final Connection conn) throws SQLException {
        String dropString = "drop table if exists RASTEST";
        String dropString2 = "drop table if exists RASTEST2";
        return executeQuery(conn, dropString, 0) && executeQuery(conn, dropString2, 0);
    }

    public void dropRasCollections() {
        RasUtil.openDatabase(RasUtil.adminUsername, RasUtil.adminPassword, true);
        RasUtil.executeRasqlQuery("drop collection rastest",
                false, true);
        RasUtil.executeRasqlQuery("drop collection rastest2",
                true, true);
    }

    private boolean executeQuery(final Connection conn, final String query, final int line) throws SQLException{
        out.printf("Executing query on line %d: %s\n... ", line, query);
        final boolean errorExpected = query.startsWith("/*e");
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeQuery(query);
        } catch (SQLException e) {
            if (!errorExpected) {
                out.println("\n>>>> Query failed! <<<<");
                e.printStackTrace();
                return false;
            }
            out.println("Success!");
            return true;
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        if (errorExpected) {
            out.println("\n>>>> Test failed! Query should have given an error, but didn't <<<<");
            return false;
        }
        out.println("Success!");
        return true;
    }

    public Connection getConnection() throws SQLException {

        Connection conn;
        Properties connectionProps = new Properties();
        connectionProps.put("user", "SA");
        connectionProps.put("password", "");

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load the hsqldb JDBCDriver", e);
        }

        final String jdbcUrl = "jdbc:hsqldb:file:" + dbFile;
        conn = DriverManager.getConnection(
                jdbcUrl,
                connectionProps
        );
        System.out.println("Connected to database: "+jdbcUrl);
        return conn;
    }
}
