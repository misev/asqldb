package org.hsqldb.test.rastests;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.hsqldb.jdbc.JDBCMArray;
import org.hsqldb.ras.RasUtil;

import rasj.RasGMArray;
import rasj.RasMArrayByte;

/**
 * Test Array and multidimensional array queries using
 * asqldb's jdbc driver
 * @author simona
 *
 */
public class RasArrayTest {
    public static final String DEFAULT_DB_FILE = "/var/hsqldb/testdb";

    private String dbFile = DEFAULT_DB_FILE;

    private PrintStream out = System.out;

    public RasArrayTest() {

    }

    public static void main(String[] args) throws SQLException {
        RasArrayTest rasArrayTest = new RasArrayTest();
        rasArrayTest.test();
    }

    public void dropRasCollections() {
        RasUtil.openDatabase(RasUtil.adminUsername, RasUtil.adminPassword, true);
        RasUtil.executeRasqlQuery("drop collection rastest",
                false, true);
        RasUtil.executeRasqlQuery("drop collection rastest2",
                false, true);
        RasUtil.executeRasqlQuery("drop collection rastest3",
                true, true);
    }

    private boolean tearDown(final Connection conn) throws SQLException {
        dropRasCollections();
        return conn == null || dropTables(conn);
    }

    //Run tests
    public void test() throws SQLException {
        boolean success = true;
        Connection conn = null;
        try {
            conn = getConnection();
            success = setUp(conn);

            //test queries:
            //			success = success && runArrayTest1(conn);
            success = success && runArrayTest2(conn);
            success = success && runArrayTest3(conn);
            success = success && runArrayTest4(conn);
            success = success && runArrayTest5(conn);
            success = success && runArrayTest6(conn);
            success = success && runArrayTest7(conn);
            //			success = success && runArrayTest8(conn);
        } catch (final SQLException e) {
            throw new RuntimeException("Tests FAILED. SQLException occurred while performing tests.", e);
        }
        finally {
            tearDown(conn);
        }

        if (success) {
            out.println("All tests PASSED");
        } else {
            out.println("Tests FAILED.");
        }
    }

    private boolean executeQuery(final Connection conn, final String query, final int line) throws SQLException{
        out.printf("Executing query on line %d: %s\n... ", line, query);
        final boolean errorExpected = query.startsWith("/*e");
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery(query);
            int columnsNumber = result.getMetaData().getColumnCount();
            if (query.startsWith("select")) {
                while (result.next()) {
                    for (int i = 1; i <= columnsNumber; i++) {
                        String colName = result.getMetaData().getColumnName(i);
//                        System.out.println("Column type: " + result.getMetaData().getColumnType(i));

                        Object colVal = (Object)result.getObject(i);
                        if (colVal instanceof RasMArrayByte) {
                            JDBCMArray jdbcArray = (JDBCMArray) result.getArray(i);
                            RasGMArray rasArray = (RasGMArray) jdbcArray.getArray();
//                            System.out.println("Type length: " + rasArray.getTypeLength());
//                            System.out.println(rasArray.getArray());
                            byte[] array = rasArray.getArray();
//                            System.out.println("Structure: " + rasArray.getTypeStructure());
//                            System.out.println("Schema: " + rasArray.getBaseTypeSchema());
//                            System.out.println(rasArray.toString());
                            /*for (int j = 0; j < array.length; j++) {
                                System.out.print(array[j] + ", ");
                            }*/
                        } else {
                            System.out.println(result.getDouble(i));
                        }
//                        System.out.println();
                    }
                }
            }
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

    public boolean dropTables(final Connection conn) throws SQLException {
        String dropString = "drop table if exists RASTEST";
        String dropString2 = "drop table if exists RASTEST2";
        String dropString3 = "drop table if exists RASTEST3";
        return executeQuery(conn, dropString, 0) 
                && executeQuery(conn, dropString2, 0)
                && executeQuery(conn, dropString3, 0);
    }

    private boolean setUp(final Connection conn) throws SQLException {
        boolean success = dropTables(conn);
        success = success && createTables(conn);
        success = success && insertValues(conn);
        return success;
    }

    public boolean createTables(final Connection conn) throws SQLException {
        String createString =
                "create table RASTEST (" +
                        "ID integer NOT NULL, " +
                        "COLL varchar(40) ARRAY NOT NULL, " +
                        "PRIMARY KEY (ID))";
        String createString2 =
                "create table RASTEST2 (" +
                        "ID integer NOT NULL, " +
                        "COLL varchar(40) ARRAY NOT NULL, " +
                        "COLL2 varchar(40) ARRAY NOT NULL, " +
                        "PRIMARY KEY (ID))";
        String createString3 =
                "create table RASTEST3 (" +
                        "ID integer NOT NULL, " +
                        "COLL varchar(40) ARRAY NOT NULL, " +
                        "COLL2 varchar(40) ARRAY NOT NULL, " +
                        "COLL3 varchar(40) ARRAY NOT NULL, " +
                        "PRIMARY KEY (ID))";
        return executeQuery(conn, createString, 0) 
                && executeQuery(conn, createString2, 0) 
                && executeQuery(conn, createString3, 0);
    }

    public boolean insertValues(final Connection conn) throws SQLException {
        RasUtil.openDatabase(RasUtil.adminUsername, RasUtil.adminPassword, true);
        RasUtil.executeRasqlQuery("create collection rastest GreySet",
                false, false);
        RasUtil.executeRasqlQuery("insert into rastest values " +
                "marray x in [0:250, 0:225] values 0c",
                false, false);
        RasUtil.executeRasqlQuery("create collection rastest2 GreySet",
                false, false);
        RasUtil.executeRasqlQuery("insert into rastest2 values " +
                "marray x in [0:225, 0:225] values 2c",
                false, false);
        RasUtil.executeRasqlQuery("create collection rastest3 GreySet",
                false, false);
        RasUtil.executeRasqlQuery("insert into rastest3 values " +
                "marray x in [0:225, 0:225] values 3c",
                true, false);

        String oidQuery = "select oid(c) from rastest as c";
        String oid = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid = oid.replaceAll("[\\[\\]]", "");
        oidQuery = "select oid(c) from rastest2 as c";
        String oid2 = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid2 = oid2.replaceAll("[\\[\\]]", "");
        oidQuery = "select oid(c) from rgb as c";
        String oid3 = RasUtil.executeRasqlQuery(oidQuery, true, false).toString();
        oid3 = oid3.replaceAll("[\\[\\]]", "");
        String[] insertQueries = new String[]{
                "INSERT INTO RASTEST VALUES(0, ARRAY['rastest:" + Double.valueOf(oid).intValue() + "'])",
                "INSERT INTO RASTEST VALUES(1, ARRAY['rastest2:" + Double.valueOf(oid2).intValue() + "'])",
                "INSERT INTO RASTEST2 VALUES(0, ARRAY['rastest:" + Double.valueOf(oid).intValue() + "']," +
                        " ARRAY['rastest2:" + Double.valueOf(oid2).intValue() + "'])",
                        "INSERT INTO RASTEST2 VALUES(1, ARRAY['rastest2:" + Double.valueOf(oid2).intValue() + "'], " +
                                "ARRAY['rastest:" + Double.valueOf(oid).intValue() + "'])",
                                "INSERT INTO RASTEST3 VALUES(0, ARRAY['rgb:" + Double.valueOf(oid3).intValue() + "'], " +
                                        "ARRAY['rgb:" + Double.valueOf(oid).intValue() + "'], " +
                                        "ARRAY['rgb:" + Double.valueOf(oid).intValue() + "'])",
                                        "INSERT INTO RASTEST3 VALUES(1, ARRAY['rgb:" + Double.valueOf(oid3).intValue() + "'], " +
                                                "ARRAY['rgb:" + Double.valueOf(oid).intValue() + "'], " +
                                                "ARRAY['rgb:" + Double.valueOf(oid).intValue() + "'])"
        };
        for (String query : insertQueries) {
            if (!executeQuery(conn, query, 0))
                return false;
        }
        return true;
    }

    // make hsqldb connection
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



    //Test 2-D array
    public boolean runArrayTest1(Connection conn) {
        boolean success_query = false;
        String query = "select id, coll[100:200, id:1+id*10] from rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }

    //Test 2-D array
    public boolean runArrayTest2(Connection conn) {
        boolean success_query = false;
        String query = "select * from rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }

    //Test 2-D array
    public boolean runArrayTest3(Connection conn) {
        boolean success_query = false;
        String query = "select id, coll[100:200, 0:10] FROM rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }

    //Test 2-D array
    public boolean runArrayTest4(Connection conn) {
        boolean success_query = false;
        String query = "SELECT id, avg_cells(coll[0:100, 50:200]) * id FROM rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }

    //Test 2-D array
    public boolean runArrayTest5(Connection conn) {
        boolean success_query = false;
        String query = "select case when avg_cells(coll)>39 then 1c else 0c end from rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }

    //Test 2-D array
    public boolean runArrayTest6(Connection conn) {
        boolean success_query = false;
        String query = "select tanh(1) from rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }

    //Test 2-D array
    public boolean runArrayTest7(Connection conn) {
        boolean success_query = false;
        String query = "select avg_cells(coll) from rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }

    //Test 2-D array
    public boolean runArrayTest8(Connection conn) {
        boolean success_query = false;
        String query = "select colval(coll) from rastest";
        try {
            success_query = executeQuery(conn, query, 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return success_query;
    }
}
