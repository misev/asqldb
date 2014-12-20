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
package org.asqldb.util;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.asqldb.ras.RasUtil;

/**
 * Manage ASQLDB/rasdaman connections: connect/disconnect, as well as query
 * execution.
 *
 * @author Dimitar Misev
 */
public class AsqldbConnection {

    public static final String HSQLDB_JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";

    private static Connection connection = null;

    public static void open(String jdbcUrl) {
        openRasConnection();
        openHsqlConnection(jdbcUrl);
    }

    public static void close() {
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

    protected static void openHsqlConnection(String jdbcUrl) {
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
     *
     * @return the first column of the first returned row, as an object.
     */
    public static Object executeQuerySingleResult(final String query) throws SQLException {
        List<Object> res = executeQuerySingleResult(query, 1);
        if (res.isEmpty()) {
            return null;
        } else {
            return res.get(0);
        }
    }

    /**
     * Execute the given query.
     *
     * @param columnCount number of columns per row returned by the query.
     * @return a list of the results from the first returned row, as objects.
     */
    public static List<Object> executeQuerySingleResult(final String query, int columnCount) throws SQLException {
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
            throw e;
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
        if (is == null) {
            System.out.println(" - failed reading test data.");
            return false;
        }
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

    public static void commit() {
        executeQuery("commit;");
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

}
