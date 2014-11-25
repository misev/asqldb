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

package org.hsqldb.test.rastests;

import org.hsqldb.ras.RasUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Base class for MDARRAY tests.
 *
 * @author Dimitar Misev
 */
public class RasBaseTest {

    public static final String DEFAULT_DB_FILE = "/var/hsqldb/test/db";
    
    private static final String HSQLDB_JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";

    public static String dbFile = DEFAULT_DB_FILE;
    public static boolean verbose = false;
    
    protected static Connection connection = null;
    
    public static void connect() {
        getRasConnection();
        getHsqlConnection();
    }
    
    public static void disconnect() {
        RasUtil.closeDatabase();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
            }
        }
    }

    public static void getRasConnection() {
        RasUtil.openDatabase(RasUtil.adminUsername, RasUtil.adminPassword, true);
    }
    
    public static void getHsqlConnection() {
        final Properties connectionProps = new Properties();
        connectionProps.put("user", "SA");
        connectionProps.put("password", "");

        try {
            Class.forName(HSQLDB_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load the hsqldb JDBCDriver", e);
        }

        final String jdbcUrl = "jdbc:hsqldb:file:" + dbFile;
        try {
            connection = DriverManager.getConnection(jdbcUrl, connectionProps);
        } catch (SQLException ex) {
            System.out.println("Failed connecting to database: " + ex.getMessage());
            throw new RuntimeException(ex);
        }
        System.out.println("Connected to database: "+jdbcUrl);
    }

    /**
     * Execute the given query, return true if passed, false otherwise.
     */
    public static boolean executeQuery(final String query) {
        System.out.print("Executing query: " + query);
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeQuery(query);
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
     * Execute the list of queries, return the number of failed ones.
     */
    public static int executeQueries(String[] queries) {
        int ret = 0;
        for (String query : queries) {
            if (!executeQuery(query)) {
                ++ret;
            }
        }
        return ret;
    }
}
