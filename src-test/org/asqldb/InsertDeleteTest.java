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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * INSERT/DELETE MDARRAY tests.<p>
 * 
 * Grammar:
 * <pre>
<array value constructor by enumeration> ::=
  ARRAY [ <array definition domain> ] <array element list>

<array element list> ::=
  <left bracket or trigraph> <array element list alt> <right bracket or trigraph>

<array element list alt> ::= 
  <array element list inner>
  | <array element list> [ { <comma> <array element list> }... ]
 
<array element list inner> ::= <array element> [ { <comma> <array element> }... ]

<array element> ::= <value expression>
   </pre>
 *
 * @author Dimitar Misev
 */
public class InsertDeleteTest extends BaseTest {
    
    protected static String[] createQueries;
    
    @BeforeClass
    public static void setUpData() {
        createQueries = new String[]{
            "create table RASTEST1 ("
                + "a DOUBLE MDARRAY[-10000:-1000])",
            "create table RASTEST2 ("
                + "a CHAR MDARRAY[x, y])"};
        dropTables(createQueries);
        createTables(createQueries);
    }
    
    @AfterClass
    public static void tearDownData() {
        dropTables(createQueries);
    }
    
    @Test
    public void testInsertArrayLiteral() {
        System.out.println("\nTest inserting array literal...");
        
        assertTrue(executeQuery("insert into RASTEST1(a) values ("
                + "MDARRAY[-9999:-9997] [1.0,2.3,-9.88832])"));
        assertEquals("{1,2.3,-9.88832}", RasUtil.collectionAsCsv("RASTEST1", "A"));
        assertTrue(executeQuery("DELETE FROM RASTEST1"));
    }
    
    @Test
    public void testInsertArrayValues() {
        System.out.println("\nTest inserting array values...");
        
        assertTrue(executeQuery("insert into RASTEST1(a) values ("
                + "MDARRAY[x(-9999:-9997)] VALUES CAST(x AS DOUBLE))"));
        assertEquals("{-9999,-9998,-9997}", RasUtil.collectionAsCsv("RASTEST1", "A"));
        assertTrue(executeQuery("DELETE FROM RASTEST1"));
        
    }
    
    @Test
    public void testInsertDecode() {
        System.out.println("\nTest inserting decode...");
        
        final InputStream is = InsertDeleteTest.class.getResourceAsStream("mr_1.png");
        executeUpdateQuery("insert into RASTEST2(a) values (mdarray_decode(?))", is);
        assertEquals("[0:255,0:210]", RasUtil.collectionAsSdom("RASTEST2", "A"));
        assertTrue(executeQuery("DELETE FROM RASTEST2"));
    }
}
