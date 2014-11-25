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

package org.asqldb.test;

import com.martiansoftware.jsap.*;
import org.asqldb.ras.RasUtil;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * This class runs the testfiles located in testrun/asqldb/select.
 * It can be expanded to run other tests.
 *
 * Created by johannes on 5/27/14.
 */
public class RasSelectTest {

    public static final String DEFAULT_DB_FILE = "/var/hsqldb/test/db";

    private String dbFile = DEFAULT_DB_FILE;
    private boolean verbose = false;

    public RasSelectTest(final JSAPResult config) {
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

        final RasSelectTest selectTest = new RasSelectTest(config);

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
        File dir = new File("testrun/asqldb/select");

        final File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String filename) {
                return filename.endsWith(".rasql");
            }
        });

        final boolean[] results = new boolean[files.length];
        int count = 0;

        System.out.print("Running tests...");
        for (int i = 0, filesLength = files.length; i < filesLength; i++) {
            File file = files[i];
            BufferedReader reader = new BufferedReader(new FileReader(file));
            results[i] = executeQuery(connection, reader.readLine());
            if (!results[i]) {
                count++;
                System.out.print('-');
            } else {
                System.out.print('+');
            }
        }

        System.out.println(String.format("%d/%d tests failed:", count, files.length));
        for (int i = 0, filesLength = files.length; i < filesLength; i++) {
            if (!results[i]) {
                System.out.println(files[i].getName());
            }
        }
        if (count > 0) {
            System.out.println(String.format("%d/%d tests failed:", count, files.length));
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
