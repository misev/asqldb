/*
 * Copyright (c) 2014, Dimitar Misev
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.asqldb;

import com.martiansoftware.jsap.*;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCMArray;
import org.asqldb.ras.RasUtil;
import org.odmg.DBag;

import rasj.RasGMArray;
import rasj.RasMArrayByte;
import rasj.RasMArrayDouble;
import rasj.RasMArrayFloat;
import rasj.RasMArrayInteger;
import rasj.RasMArrayLong;
import rasj.RasMArrayShort;
import rasj.RasMInterval;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

/**
 * This class runs the testfiles located in testrun/asqldb/select.
 * It can be expanded to run other tests.
 *
 * Created by johannes on 5/27/14.
 */
public class RasMArrayTest {

    public static final String DEFAULT_DB_FILE = "/var/hsqldb/test/db";

    private String dbFile = DEFAULT_DB_FILE;
    private boolean verbose = false;

    public RasMArrayTest(final JSAPResult config) {
        dbFile = config.getString("dbFile");
    }


    public static void main(String[] args) throws SQLException, JSAPException {
        JSAP jsap = new JSAP();

        FlaggedOption opt1 = new FlaggedOption("dbFile")
        .setStringParser(JSAP.STRING_PARSER)
        .setDefault(DEFAULT_DB_FILE)
        .setRequired(false)
        .setShortFlag('d')
        .setLongFlag("database-file");

        jsap.registerParameter(opt1);

        UnflaggedOption opt3 = new UnflaggedOption("files")
        .setStringParser(JSAP.STRING_PARSER)
        .setRequired(false)
        .setGreedy(true);

        jsap.registerParameter(opt3);

        JSAPResult config = jsap.parse(args);

        final RasMArrayTest selectTest = new RasMArrayTest(config);

        try {
            selectTest.run();
        } catch (IOException e) {
            throw new RuntimeException("Could not read files.", e);
        }

        // check whether the command line was valid, and if it wasn't,
        // display usage information and exit.
        if (!config.success()) {
            System.err.println("\nUsage: java ...\n" + jsap.getUsage() + "\n");
            System.exit(1);
        }
    }

    public void run() throws SQLException, IOException {
        final Connection connection = getConnection();
        setUp(connection);

        String asqldbFolder = "testrun/asqldb/select";
        String rasqlFolder = "testrun/asqldb/selectRasj";

        File dirAsqldb = new File(asqldbFolder);

        final File[] asqldbFiles = dirAsqldb.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String filename) {
                return filename.endsWith(".rasql");
            }
        });

        final boolean[] results = new boolean[asqldbFiles.length];
        int count = 0;
        int totalTests = 0;
        System.out.print("Running tests...");
        for (int i = 0, filesLength = asqldbFiles.length; i < filesLength; i++) {
            File file = asqldbFiles[i];
            String fileName = file.getName();
            File rasqlFile = new File(rasqlFolder + "/" + fileName);
            if (!rasqlFile.exists())
                continue;
            totalTests++;
            BufferedReader reader = new BufferedReader(new FileReader(file));
            BufferedReader reader1 = new BufferedReader(new FileReader(rasqlFile));

            String asqldbQuery = reader.readLine();
            String rasqlQuery = reader1.readLine();

            Object asqlResult = getAsqlResult(connection, asqldbQuery);
            Object rasqlResult = getRasqlResult(rasqlQuery);

            System.out.println("Comparing results for : " + asqldbQuery + "   " + rasqlQuery);
            results[i] = compareResults(asqlResult, rasqlResult);
            System.out.println("Rezultatul este " + results[i]);
            if (!results[i]) {
                count++;
                System.out.print('-');
            } else {
                System.out.print('+');
            }
        }

        System.out.println(String.format("%d/%d tests failed:", count, totalTests));
        for (int i = 0, filesLength = asqldbFiles.length; i < filesLength; i++) {
            File rasqlFile = new File(rasqlFolder + "/" + asqldbFiles[i].getName());
            if (!rasqlFile.exists())
                continue;

            if (!results[i]) {
                System.out.println(asqldbFiles[i].getName());
            }
        }

        if (count > 0) {
            System.out.println(String.format("%d/%d tests failed:", count, totalTests));
            System.out.println("--- TESTS FAILED ---");
        } else {
            System.out.println("+++ All tests PASSED +++");
        }
    }

    private void setUp(final Connection connection) throws SQLException {

        RasUtil.openDatabase(RasUtil.adminUsername, RasUtil.adminPassword, true);
        try {
            RasUtil.executeRasqlQuery("create collection rastest GreySet",
                    false, false);
            RasUtil.executeRasqlQuery("insert into rastest values marray x in [0:250, 0:250] values 1c",
                    false, false);
            RasUtil.executeRasqlQuery("create collection rastest2 GreySet",
                    false, false);
            RasUtil.executeRasqlQuery("insert into rastest2 values marray x in [0:250, 0:250] values 2c",
                    true, false);
        } catch (Exception ignored) {  }

        String oidQuery = "select oid(c) from rastest as c";
        String oid = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid = oid.replaceAll("[\\[\\]]", "");
        oidQuery = "select oid(c) from rastest2 as c";
        String oid2 = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid2 = oid2.replaceAll("[\\[\\]]", "");
        oidQuery = "select oid(c) from rgb as c";
        String oid3 = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid3 = oid3.replaceAll("[\\[\\]]", "");

        String[] queries = new String[]{
                "create table RASTEST (ID integer NOT NULL, COLL varchar(40) ARRAY NOT NULL, PRIMARY KEY (ID))",
                "create table RASTEST2 (ID integer NOT NULL, COLL varchar(40) ARRAY NOT NULL, " +
                        "COLL2 varchar(40) ARRAY NOT NULL, PRIMARY KEY (ID))",
                        "create table RGB (ID integer NOT NULL, COLL varchar(40) ARRAY NOT NULL, " +
                                "COLL2 varchar(40) ARRAY NOT NULL, PRIMARY KEY (ID))",
                                "INSERT INTO RASTEST VALUES(0, ARRAY['rastest:" + Double.valueOf(oid).intValue() + "'])",
                                "INSERT INTO RASTEST2 VALUES(0, ARRAY['rastest:" + Double.valueOf(oid).intValue() + "'], " +
                                        "ARRAY['rastest2:" + Double.valueOf(oid2).intValue() + "'])",
                                        "INSERT INTO RGB VALUES(0, ARRAY['rgb:" + Double.valueOf(oid3).intValue() + "'], " +
                                                "ARRAY['rgb:" + Double.valueOf(oid3).intValue() + "'])",
        };
        for (String query : queries) {
            executeQuery(connection, query);
        }
    }

    
    private boolean executeQuery(final Connection conn, final String query) throws SQLException{
        if (verbose) System.out.printf("Executing query: "+ query);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeQuery(query);
        } catch (SQLException e) {
            return false;
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        return true;
    }

    /**
     * Determine if the object is a rasdaman multidimensional
     * array object
     * @param obj: The object returned in the ResultSet
     * @return true if the object is instance of any
     *         of the RasMArray* types
     */
    private boolean isArrayColumn(Object obj) {
        if (obj instanceof RasGMArray)
            return true;
        if (obj instanceof RasMArrayByte)
            return true;
        if (obj instanceof RasMArrayDouble)
            return true;
        if (obj instanceof RasMArrayFloat)
            return true;
        if (obj instanceof RasMArrayInteger)
            return true;
        if (obj instanceof RasMArrayLong)
            return true;
        if (obj instanceof RasMArrayShort)
            return true;
        return false;
    }

    /**
     * 
     * @param conn
     * @param query: The asqldb query
     * @return The object returned by executing the query using the JDBC
     * @throws SQLException
     */
    private Object getAsqlResult(final Connection conn, final String query) throws SQLException {
        if (verbose) System.out.printf("Executing asql query: "+ query);
        Statement   stmt = null;
        byte[]      array = null;
        try {
            stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery(query);

            if (result.next()) {
                Object colVal = (Object)result.getObject(1);
                return colVal;
            }
        } catch (SQLException e) {
            return array;
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        return array;
    }

    /**
     * Execute a rasql query using rasj
     * @param query: The rasql query
     * @return The result return by rasdaman for the query
     */
    private Object getRasqlResult(String query) {
        if (verbose) System.out.printf("Executing rasql query: " + query);
        Object result = RasUtil.executeRasqlQuery(query, false, false);
        DBag dbag = (DBag)result;

        final Iterator it = dbag.iterator();
        if (!(it.hasNext()))
            throw Error.error(ErrorCode.MDA_OIDNOTFOUND, query);

        final Object obj = it.next();

        return obj;
    }

    /**
     * Compare an asqldb result and a rasql result
     * @param asqlResult
     * @param rasqlResult
     * @return
     */
    private boolean compareResults(Object asqlResult, Object rasqlResult) {
        if (asqlResult == null && rasqlResult == null) {
            return true;
        }

        if (asqlResult == null || rasqlResult == null) {
            return false;
        }

        // compare intervals
        if (asqlResult instanceof RasMInterval &&
                rasqlResult instanceof RasMInterval) {
            RasMInterval asqlInterval = (RasMInterval)asqlResult;
            RasMInterval rasqlInterval = (RasMInterval)rasqlResult;

            return asqlInterval.equals(rasqlInterval);
        }

        //compare array results
        if (isArrayColumn(asqlResult) && isArrayColumn(rasqlResult)) {
            return compareArrays(asqlResult, rasqlResult);
        } else {
            // compare non array results - only numerical values
            if (!isArrayColumn(asqlResult) && !isArrayColumn(rasqlResult)) {
                Double resAsql = Double.parseDouble(asqlResult.toString());
                Double resRasql = Double.parseDouble(rasqlResult.toString());
                return resAsql.equals(resRasql);
            }
        }
        return false;
    }

    /**
     * compare two rasdaman multidimensional arrays
     * @param asqlResult
     * @param rasqlResult
     * @return
     */
    public boolean compareArrays(Object asqlResult, Object rasqlResult) {
        System.out.println("Intra aici");
        RasGMArray asqlArray = (RasGMArray) asqlResult;
        RasGMArray rasqlArray = (RasGMArray) rasqlResult;

        byte[] asql = asqlArray.getArray();
        byte[] rasql = rasqlArray.getArray();

        if (asql.length != rasql.length) {
            return false;
        }

        for (int i = 0; i < asql.length; i++) {
            if (asql[i] != rasql[i]) {
                System.out.println("Difera ceva " + i);
                return false;
            }
        }
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
