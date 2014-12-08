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
import org.asqldb.util.AsqldbConnection;
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
public class BaseTest extends AsqldbConnection {

    public static final String DEFAULT_DB_PATH = "mem:test;sql.enforce_strict_size=true";
//    public static final String DEFAULT_DB_PATH = "file:/home/dimitar/tmp/db/personal;shutdown=true";

    protected static String dbPath = DEFAULT_DB_PATH;
    protected static String jdbcUrl = "jdbc:hsqldb:" + dbPath;
    
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
            if (e.getMessage() != null) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    };
    
    @BeforeClass
    public static void setUp() {
        AsqldbConnection.open(jdbcUrl);
    }
    
    @AfterClass
    public static void tearDown() {
        AsqldbConnection.close();
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
                + "a CHAR MDARRAY[x, y])",
            "create table RASTEST3 ("
                + "id INTEGER,"
                + "a CHAR MDARRAY[x])"};
        dropTables(createQueries);
        createTables(createQueries);
        
        executeQuery("insert into RASTEST1(a) values ("
                + "MDARRAY[-9999:-9997] [1.0,2.3,-9.88832])");
        
        final InputStream is = InsertDeleteTest.class.getResourceAsStream("mr_1.png");
        executeUpdateQuery("insert into RASTEST2(a) values (mdarray_decode(?))", is);
        
        executeQuery("insert into RASTEST3(id, a) values ("
                + "3, MDARRAY[x(0:2)] [2,5,3])");
        executeQuery("insert into RASTEST3(id, a) values ("
                + "2, MDARRAY[x(0:1)] [8,7])");
        
        return createQueries;
    }
}
