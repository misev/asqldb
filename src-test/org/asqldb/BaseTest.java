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

import java.io.InputStream;
import org.asqldb.ras.RasUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Base class for MDARRAY tests.
 *
 * @author Dimitar Misev
 */
public class BaseTest {

    public static final String DEFAULT_DB_PATH = "mem:test;sql.enforce_strict_size=true";
    public static final String HSQLDB_JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";

    protected static String dbPath = DEFAULT_DB_PATH;
    protected static String jdbcUrl = "jdbc:hsqldb:" + dbPath;
    
    protected static Connection connection = null;
    
    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            System.out.println("\n-----------------------------------------------------------------------");
            System.out.println("Running test: " + description.getMethodName());
        }
        
        @Override
        protected void succeeded(Description description) {
            System.out.println("TEST PASSED");
        }
        
        @Override
        protected void failed(Throwable e, Description description) {
            System.out.println("*** TEST FAILED ***");
            System.out.println("Error: " + e.getMessage());
        }
    };
    
    @BeforeClass
    public static void setUp() {
        connect();
    }
    
    @AfterClass
    public static void tearDown() {
        disconnect();
    }
    
    protected static void connect() {
        openRasConnection();
        openHsqlConnection();
    }
    
    protected static void disconnect() {
        RasUtil.closeDatabase();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
            } finally {
                connection = null;
            }
        }
    }

    protected static void openRasConnection() {
        RasUtil.openDatabase(RasUtil.adminUsername, RasUtil.adminPassword, true);
    }
    
    protected static void openHsqlConnection() {
        try {
            Class.forName(HSQLDB_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load the hsqldb JDBCDriver", e);
        }

        try {
            connection = DriverManager.getConnection(jdbcUrl, getJdbcConnectionProperties());
        } catch (SQLException ex) {
            throw new RuntimeException("Failed getting JDBC connection.", ex);
        }
    }
    
    private static Properties getJdbcConnectionProperties() {
        final Properties ret = new Properties();
        ret.put("user", "SA");
        ret.put("password", "");
        return ret;
    }

    /**
     * Execute the given query, return true if passed, false otherwise.
     */
    public static boolean executeQuery(final String query) {
        System.out.print("  executing query: " + query);
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery(query);
        } catch (SQLException e) {
            System.out.println(" ... failed.");
            e.printStackTrace();
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        System.out.println(" ... ok.");
        return true;
    }

    /**
     * Execute the given query.
     * @return the first column of the first returned row, as an object.
     */
    public static Object executeQuerySingleResult(final String query) {
        List<Object> res = executeQuerySingleResult(query, 1);
        if (res.isEmpty()) {
            return null;
        } else {
            return res.get(0);
        }
    }

    /**
     * Execute the given query.
     * @param columnCount number of columns per row returned by the query.
     * @return a list of the results from the first returned row, as objects.
     */
    public static List<Object> executeQuerySingleResult(final String query, int columnCount) {
        System.out.print("  executing query: " + query);
        List<Object> ret = new ArrayList<Object>();
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    ret.add(rs.getObject(i));
                }
            }
        } catch (SQLException e) {
            System.out.println(" ... failed.");
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        System.out.println(" ... ok.");
        return ret;
    }

    /**
     * Execute the given query, return true if passed, false otherwise.
     */
    public static boolean executeUpdateQuery(final String query, final InputStream is) {
        System.out.print("  executing update query: " + query);
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(query);
            stmt.setBlob(1, is);
            int rows = stmt.executeUpdate();
            if (rows <= 0) {
                System.out.println(" ... failed.");
            }
        } catch (SQLException e) {
            System.out.println(" ... failed.");
            e.printStackTrace();
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        System.out.println(" ... ok.");
        return true;
    }
    
    /**
     * Execute the list of queries, return the number of passed ones.
     */
    public static int executeQueries(String[] queries) {
        int ret = 0;
        for (String query : queries) {
            if (executeQuery(query)) {
                ++ret;
            }
        }
        return ret;
    }
    
    public static void printCheck(boolean res) {
        if (res) {
            System.out.println("ok.");
        } else {
            System.out.println("failed.");
        }
    }
    
    public static void printCheck(boolean res, String msg) {
        System.out.print(msg + "... ");
        printCheck(res);
    }
    
    public static int createTables(final String[] queries) {
        System.out.println("\nCreating tables...");
        return executeQueries(queries);
    }
    
    /**
     * Drop tables, assumes that the tables are called RASTEST1, RASTEST2, ...
     * 
     * @return true if tables have been successfully removed from rasdaman,
     * false otherwise.
     */
    public static boolean dropTables(final String[] queries) {
        System.out.println("\nDroping tables...");
        boolean ret = false;
        for (int i = 1; i <= queries.length; i++) {
            final String table = "RASTEST" + i;
            executeQuery("DROP TABLE " + table + " IF EXISTS");
            ret = ret || tableExistsInRasdaman(table);
        }
        return !ret;
    }
    
    /**
     * Assumes the corresponding array field in HSQLDB is A.
     */
    public static boolean tableExistsInRasdaman(String table) {
        return RasUtil.collectionExists("PUBLIC_" + table + "_A");
    }
    
    public static String[] insertTestData() {
        final String[] createQueries = new String[]{
            "create table RASTEST1 ("
                + "a DOUBLE MDARRAY[-10000:-1000])",
            "create table RASTEST2 ("
                + "a CHAR MDARRAY[x, y])"};
        dropTables(createQueries);
        createTables(createQueries);
        
        executeQuery("insert into RASTEST1(a) values ("
                + "MDARRAY[-9999:-9997] [1.0,2.3,-9.88832])");
        
        final InputStream is = InsertDeleteTest.class.getResourceAsStream("mr_1.png");
        executeUpdateQuery("insert into RASTEST2(a) values (mdarray_decode(?))", is);
        
        return createQueries;
    }
}
