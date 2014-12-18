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
import org.junit.Assert;
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
                + "b DOUBLE ARRAY[5])"};
        dropTables(createQueries);
        createTables(createQueries);
        
        executeQuery("insert into RASTEST1(a,b) values ("
                + "MDARRAY[-9999:-9997] [1.0,2.3,-9.88832],"
                + "ARRAY[5,4,3,2,1])");
        
        executeQuery("insert into RASTEST1(a,b) values ("
                + "MDARRAY[-9999:-9997] [11.0,21.3,-91.88832],"
                + "ARRAY[15,14,13,12,11])");
    }
    
    @AfterClass
    public static void tearDownData() {
        dropTables(createQueries);
    }
    
    @Test
    public void testArrayUnnest() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v from RASTEST1 as c, UNNEST(c.b) as t(v)", 1);
        Assert.assertEquals(10, o.size());
    }
    
    @Test
    public void testArrayUnnestWithOrdinality() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v, ord from RASTEST1 as c, UNNEST(c.b) with ordinality as t(v, ord)", 2);
        Assert.assertEquals(20, o.size());
    }
    
    @Test
    public void testMDArrayUnnest() throws SQLException {
        List<Object> o = executeQuerySingleResult("select v from RASTEST1 as c, UNNEST(c.a) as t(v)", 1);
        Assert.assertEquals(6, o.size());
    }

    public static void main(String[] args) throws SQLException {
        UnnestTest test = new UnnestTest();
        AsqldbConnection.open(jdbcUrl);
        test.testArrayUnnest();
    }
}
