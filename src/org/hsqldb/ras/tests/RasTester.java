package org.hsqldb.ras.tests;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by Johannes on 5/5/14.
 *
 * @author Johannes Bachhuber
 */
public class RasTester {

    private static PrintStream out = System.out;


    public static void main(String[] args) throws SQLException{
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = getConnection();

        boolean success = dropTables(conn);

        //test setup:
        success = success && createTables(conn);
        success = success && insertValues(conn);

        //test queries:
        success = success && runTests(conn);

        //test cleanup:
        success = success && dropTables(conn);

        if (success) {
            out.println("All tests PASSED");
        } else {
            out.println("Tests FAILED.");
        }
    }

    public static boolean createTables(Connection conn) throws SQLException {
        String createString =
                "create table RASTEST (ID integer NOT NULL, COLL varchar(40) ARRAY NOT NULL, PRIMARY KEY (ID))";
        return executeUpdate(conn, createString);
    }

    public static boolean insertValues(Connection conn) throws SQLException {
        String createString =
                "INSERT INTO RASTEST VALUES(0, ARRAY['rgb:17409'])";
        return executeUpdate(conn, createString);
    }

    public static boolean runTests(Connection conn) throws SQLException {
        String[] queries = new String[]{
                "select * from rastest",
                "select png(coll) from rastest",
                "select id, png(coll) as collpath from rastest",
                "select id, coll[100:200, 30:150] from rastest",
                "select id, csv(coll[100:200, 3]) from rastest",
                "select id, coll[100:200, id] from rastest",
                "select id, coll[100:200, id + 1] from rastest",
                "select id, coll[100:200, id:1+id*10] from rastest",
                "select id, coll[100:200, id:1+id*10] + 10 from rastest"
        };


        for (String query : queries) {
            if (!executeQuery(conn, query))
                return false;
        }
        return true;
    }

    public static boolean dropTables(Connection conn) throws SQLException {
        String createString = "drop table if exists RASTEST";
        return executeUpdate(conn, createString);
    }

    private static boolean executeUpdate(Connection conn, String query) throws SQLException{
        out.print("Executing update: " + query + "\n... ");
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
        } catch (SQLException e) {
            out.println("\n>>>> Update failed! <<<<");
            e.printStackTrace();
            return false;
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        out.println("Success!");
        return true;
    }

    private static boolean executeQuery(Connection conn, String query) throws SQLException{
        out.print("Executing query: " + query + "\n... ");
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeQuery(query);
        } catch (SQLException e) {
            out.println("\n>>>> Query failed! <<<<");
            e.printStackTrace();
            return false;
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        out.println("Success!");
        return true;
    }

    public static Connection getConnection() throws SQLException {

        Connection conn;
        Properties connectionProps = new Properties();
        connectionProps.put("user", "SA");
        connectionProps.put("password", "");

        conn = DriverManager.getConnection(
                "jdbc:hsqldb:file:/var/hsqldb/db",
                connectionProps
        );
        System.out.println("Connected to database");
        return conn;
    }
}
