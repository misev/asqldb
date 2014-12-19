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

import java.sql.SQLException;
import java.util.List;
import org.asqldb.util.AsqldbConnection;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test UNNEST operation on MDARRAY.
 *
 * @author Dimitar Misev
 */
public class UnnestTest extends BaseTest {
    
    protected static String[] createQueries;
    
    @BeforeClass
    public static void setUpData() {
        
        createQueries = new String[]{
            "create table RASTEST1 ("
                + "a DOUBLE MDARRAY[-10000:-1000],"
                + "b DOUBLE ARRAY[5])",
            "create table RASTEST2 ("
                + "a DOUBLE MDARRAY[x,y],"
                + "b DOUBLE MDARRAY[x,y])",
            "create table RASTEST3 ("
                + "id INTEGER)"};
        dropTables(createQueries);
        createTables(createQueries);
        
        executeQuery("insert into RASTEST1(a,b) values ("
                + "MDARRAY[-9999:-9997] [1.0,2.3,-9.88832],"
                + "ARRAY[5,4,3,2,1])");
        
        executeQuery("insert into RASTEST1(a,b) values ("
                + "MDARRAY[-9999:-9998] [11.0,21.3],"
                + "ARRAY[15,14,13,12,11])");
        
        executeQuery("insert into RASTEST2(a,b) values ("
                + "MDARRAY[1:1,1:3] [1.0,2.3,-9.88832],"
                + "MDARRAY[1:1,2:4] [11.0,21.3,-91.88832])");
        
        executeQuery("insert into RASTEST2(a,b) values ("
                + "MDARRAY[1:3,2:2] [12.0,22.3,-92.88832],"
                + "MDARRAY[2:3,2:3] [13.0,23.3,-93.88832,15.0])");
        
        executeQuery("insert into RASTEST3(id) values (1)");
        executeQuery("insert into RASTEST3(id) values (2)");
        executeQuery("insert into RASTEST3(id) values (3)");
    }
    
    @AfterClass
    public static void tearDownData() {
        dropTables(createQueries);
    }
    
    @Test
    public void testArrayUnnest() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v from RASTEST1 as c, UNNEST(c.b) as t(v)", 1);
        assertEquals(10, o.size());
    }
    
    @Test
    public void testArrayUnnestWithOrdinality() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v, ord from RASTEST1 as c, UNNEST(c.b) with ordinality as t(v, ord)", 2);
        assertEquals(20, o.size());
    }
    
    @Test
    public void testMDArrayUnnest() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v from RASTEST1 as c, UNNEST(c.a) as t(v)", 1);
        assertEquals(5, o.size());
    }
    
    @Test
    public void testMDArrayUnnestWithOrdinality() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v, ord from RASTEST1 as c, UNNEST(c.a) with ordinality as t(v, ord)", 2);
        assertEquals(10, o.size());
        assertEquals("[1.0, -9999, 2.3, -9998, -9.88832, -9997, 11.0, -9999, 21.3, -9998]", o.toString());
    }
    
    @Test
    public void testMDArrayUnnest2d() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v from RASTEST2 as c, UNNEST(c.a) as t(v)", 1);
        assertEquals(6, o.size());
        assertEquals("[1.0, 2.3, -9.88832, 12.0, 22.3, -92.88832]", o.toString());
        
        o = executeQuerySingleResult("select v,w from RASTEST2 as c, UNNEST(c.a, c.b) as t(v,w)", 2);
        assertEquals(14, o.size());
        assertEquals("[1.0, 11.0, 2.3, 21.3, -9.88832, -91.88832, 12.0, 13.0, 22.3, 23.3, -92.88832, -93.88832, null, 15.0]", o.toString());
    }
    
    @Test
    public void testMDArrayUnnest2dWithOrdinality() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v,x,y from RASTEST2 as c, UNNEST(c.a) with ordinality as t(v,x,y)", 3);
        assertEquals(18, o.size());
        assertEquals("[1.0, 1, 1, 2.3, 1, 2, -9.88832, 1, 3, 12.0, 1, 2, 22.3, 2, 2, -92.88832, 3, 2]", o.toString());
        
        o = executeQuerySingleResult("select v,x,y from RASTEST2 as c, UNNEST(c.b) with ordinality as t(v,x,y)", 3);
        assertEquals(21, o.size());
        assertEquals("[11.0, 1, 2, 21.3, 1, 3, -91.88832, 1, 4, 13.0, 2, 2, 23.3, 2, 3, -93.88832, 3, 2, 15.0, 3, 3]", o.toString());
        
        o = executeQuerySingleResult("select v,w,x,y from RASTEST2 as c, UNNEST(c.a, c.b) with ordinality as t(v,w,x,y)", 4);
        assertEquals(28, o.size());
        assertEquals("[1.0, 11.0, 1, 1, 2.3, 21.3, 1, 2, -9.88832, -91.88832, 1, 3, "
                + "12.0, 13.0, 2, 2, 22.3, 23.3, 2, 3, -92.88832, -93.88832, 3, 2, null, 15.0, 3, 3]", o.toString());
    }
    
    @Test
    public void testArrayNest() throws SQLException {
        List<Object> o = executeQuerySingleResult("select ARRAY (SELECT a.id FROM RASTEST3 as a ORDER BY id), c.b[1] from RASTEST1 as c", 2);
        assertEquals(4, o.size());
    }

    public static void main(String[] args) throws SQLException {
        UnnestTest test = new UnnestTest();
        AsqldbConnection.open(jdbcUrl);
        test.testArrayUnnest();
    }
}
