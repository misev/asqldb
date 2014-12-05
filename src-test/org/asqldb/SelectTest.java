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
import org.asqldb.ras.RasUtil;
import rasj.RasMArrayByte;
import rasj.RasMArrayDouble;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import rasj.RasMArrayInteger;

/**
 * Run SQL/MDA select tests.
 *
 * @author Dimitar Misev
 */
public class SelectTest extends BaseTest {
    
    protected static String[] createQueries;
    
    @BeforeClass
    public static void setUpData() {
        createQueries = insertTestData();
    }
    
    @AfterClass
    public static void tearDownData() {
        dropTables(createQueries);
    }
    
    /**
     * Test simple, single array select.
     */
    
    @Test
    public void testSingleArraySelect() throws SQLException {
        Object dbag = executeQuerySingleResult("select c.a from RASTEST1 as c");
        RasMArrayDouble res = (RasMArrayDouble) RasUtil.head(dbag);
        double[] d = res.getDoubleArray();
        assertEquals(d.length, 3);
    }
    
    @Test
    public void testSingleArrayEncode() throws SQLException {
        Object dbag = executeQuerySingleResult("select mdarray_encode(c.a, 'PNG') from RASTEST2 as c");
        RasMArrayByte res = (RasMArrayByte) RasUtil.head(dbag);
        byte[] d = res.getArray();
        assertEquals(d.length, 22624);
    }
    
    /**
     * Test predefined aggregation operators:
     * avg_cells, add_cells, count_cells, some_cells, all_cells
     */
    
    @Test
    public void testPredefinedAggregation_AvgCells() throws SQLException {
        Double hsqlRes = (Double) executeQuerySingleResult(
                "select avg_cells(c.a) from RASTEST2 as c");
        Double rasqlRes = (Double) RasUtil.head(RasUtil.executeRasqlQuery(
                "select avg_cells(c) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes, hsqlRes);
    }
    
    @Test
    public void testPredefinedAggregation_AddCells() throws SQLException {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select add_cells(c.a = 0) from RASTEST2 as c");
        Long rasqlRes = (Long) RasUtil.head(RasUtil.executeRasqlQuery(
                "select add_cells(c = 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes.longValue(), hsqlRes.longValue());
    }
    
    @Test
    public void testPredefinedAggregation_CountCells() throws SQLException {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select count_cells(c.a != 0) from RASTEST2 as c");
        Long rasqlRes = (Long) RasUtil.head(RasUtil.executeRasqlQuery(
                "select count_cells(c != 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes.longValue(), hsqlRes.longValue());
    }
    
    @Test
    public void testPredefinedAggregation_SomeCells() throws SQLException {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select some_cells(c.a > 0) from RASTEST2 as c");
        Integer rasqlRes = (Integer) RasUtil.head(RasUtil.executeRasqlQuery(
                "select some_cells(c > 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes, hsqlRes);
    }
    
    @Test
    public void testPredefinedAggregation_AllCells() throws SQLException {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select all_cells(c.a <= 0) from RASTEST2 as c");
        Integer rasqlRes = (Integer) RasUtil.head(RasUtil.executeRasqlQuery(
                "select all_cells(c <= 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes == 0);
        assertEquals(rasqlRes, hsqlRes);
    }
    
    /**
     * Test general array constructor
     */
    
    @Test
    public void testGeneralArrayConstructor_Const() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select mdarray [x(0:10),y(0:10)] values 1 from RASTEST1");
        int[] d = res.getIntArray();
        assertEquals(d.length, 121);
    }
    
    @Test
    public void testGeneralArrayConstructor_IteratorVars1() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select mdarray [x(0:10),y(0:10)] values x + y from RASTEST1");
        int[] d = res.getIntArray();
        assertEquals(121, d.length);
        assertEquals(5, d[5]);
    }
    
    @Test
    public void testGeneralArrayConstructor_IteratorVars2() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select mdarray [x(-1:1)] values 2 * x from RASTEST1");
        int[] d = res.getIntArray();
        assertEquals(3, d.length);
        assertEquals(-2, d[0]);
    }
    
    @Test
    public void testGeneralArrayConstructor_HsqlMix1() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select mdarray [x(0:2)] values id * x from RASTEST3");
        int[] d = res.getIntArray();
        assertEquals(3, d.length);
        assertEquals(6, d[2]);
    }
    
    @Test
    public void testGeneralArrayConstructor_HsqlMix2() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select mdarray [x(0:2), x1(0:2)] values xid.id * x + x1 from RASTEST3 as xid");
        int[] d = res.getIntArray();
        assertEquals(9, d.length);
        assertEquals(2, d[2]);
    }
    
    @Test
    public void testGeneralArrayConstructor_InvalidIterator() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select mdarray [x(50:55)] values 2 + y from RASTEST1");
            fail();
        } catch (SQLException ex) {
        }
    }
    
    @Test
    public void testGeneralArrayConstructor_Nested() throws SQLException {
        RasMArrayDouble res = (RasMArrayDouble) executeQuerySingleResult(
                    "select mdarray [x(50:55)] values avg_cells(mdarray [y(10:20)] values x+y) from RASTEST1");
        assertEquals(6, res.getDoubleArray().length);
    }
    
    /**
     * Test general array aggregate
     */
    
    @Test
    public void testGeneralArrayAggregate_Const() throws SQLException {
        Integer res = (Integer) executeQuerySingleResult(
                "select aggregate + over [x(0:10),y(0:10)] using 1 from RASTEST1");
        assertEquals(121, res.intValue());
    }
    
    @Test
    public void testGeneralArrayAggregate_IteratorVars1() throws SQLException {
        Integer res = (Integer) executeQuerySingleResult(
                "select aggregate + over [x(0:10),y(0:10)] using x from RASTEST1");
        assertEquals(605, res.intValue());
    }
    
    @Test
    public void testGeneralArrayAggregate_InvalidIterator() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select aggregate + over [x(0:10)] using y from RASTEST1");
            fail();
        } catch (SQLException ex) {
        }
    }
    
    @Test
    public void testGeneralArrayAggregate_ArrayConstructorMix() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select mdarray [z(0:2)] values aggregate + over [x(0:10),y(0:10)] using z from RASTEST1");
        int[] d = res.getIntArray();
        assertEquals(3, d.length);
        assertEquals(242, d[2]);
    }
    
    /**
     * Test array subset
     */
    
    @Test
    public void testSubset() throws SQLException {
        RasMArrayDouble res = (RasMArrayDouble) executeQuerySingleResult(
                "select a[-9999:-9998] from RASTEST1");
        double[] d = res.getDoubleArray();
        assertEquals(2, d.length);
        assertEquals(2.3, d[1], 0.01);
    }
    
    @Test
    public void testSubset2() throws SQLException {
        RasMArrayDouble res = (RasMArrayDouble) executeQuerySingleResult(
                "select a[+9999:10000] from RASTEST1");
        double[] d = res.getDoubleArray();
        assertEquals(2, d.length);
        assertEquals(0, d[1], 0.01);
    }
    
    @Test
    public void testSubset3() throws SQLException {
        RasMArrayDouble res = (RasMArrayDouble) executeQuerySingleResult(
                "select a[+9999*1:10000] from RASTEST1");
        double[] d = res.getDoubleArray();
        assertEquals(2, d.length);
        assertEquals(0, d[1], 0.01);
    }
    
    @Test
    public void testSlice() throws SQLException {
        Double res = (Double) executeQuerySingleResult(
                "select a[-9998] from RASTEST1");
        assertEquals(2.3, res.doubleValue(), 0.01);
    }
    
    @Test
    public void testSubsetSlice() throws SQLException {
        RasMArrayByte res = (RasMArrayByte) executeQuerySingleResult(
                "select a[50:55,100] from RASTEST2");
        byte[] d = res.getArray();
        assertEquals(6, d.length);
        assertEquals(22, d[2]);
    }
    
    @Test
    public void testSubsetDimensionNames() throws SQLException {
        Integer res = (Integer) executeQuerySingleResult(
                "select a[x(100),y(100)] from RASTEST2");
        assertEquals(163, res.intValue());
    }
    
    @Test
    public void testSubsetDimensionName1() throws SQLException {
        Double res = (Double) executeQuerySingleResult(
                "select avg_cells(a[y(100)]) from RASTEST2");
        assertEquals(81, res.intValue());
    }
    
    @Test
    public void testSubsetDimensionName2() throws SQLException {
        Double res = (Double) executeQuerySingleResult(
                "select avg_cells(a[x(80)]) + avg_cells(a[y(50)]) from RASTEST2");
        assertEquals(29, res.intValue());
    }
    
    @Test
    public void testSubsetUnbounded() throws SQLException {
        Double res = (Double) executeQuerySingleResult(
                "select avg_cells(a[80,*:*]) + avg_cells(a[0:*,50]) from RASTEST2");
        assertEquals(29, res.intValue());
    }
    
    @Test
    public void testSubsetMixedNames() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select avg_cells(a[80,x(*:*)]) + avg_cells(a[y(50),*:*]) from RASTEST2");
            fail();
        } catch (Exception ex) {
            assertEquals("Invalid subset specification: Invalid mixing of names and positional indexes in [80,X(*:*)]", ex.getMessage());
        }
    }
    
    @Test
    public void testSubsetInvalidDimensionName() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select a[x(100),z(100)] from RASTEST2");
            fail();
        } catch (Exception ex) {
            assertEquals("Invalid subset specification: Referenced an unknown dimension name: Z", ex.getMessage());
        }
    }
    
    @Test
    public void testSubsetInvalidIndex1() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select a[x(asdf)] from RASTEST2");
            fail();
        } catch (Exception ex) {
            assertEquals("user lacks privilege or object not found: ASDF", ex.getMessage());
        }
    }
    
    @Test
    public void testSubsetInvalidIndex2() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select a[a:1,0:*] from RASTEST2");
            fail();
        } catch (Exception ex) {
            assertEquals("incompatible data type in operation", ex.getMessage());
        }
    }
    
    @Test
    public void testSubsetInvalidIndex3() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select a[0:*:1,21:12] from RASTEST2");
            fail();
        } catch (Exception ex) {
            assertEquals("unexpected token: : required: ,", ex.getMessage());
        }
    }
    
    @Test
    public void testSubsetInvalidIndex4() throws SQLException {
        try {
            executeQuerySingleResult(
                    "select a[x(21:12),y(5)] from RASTEST2");
            fail();
        } catch (Exception ex) {
            assertEquals("Error executing the rasdaman query: SELE", ex.getMessage().substring(0,40));
        }
    }
    
    /**
     * Test probing functions
     */
    
    @Test
    public void testLoIndex() throws SQLException {
        Integer lo = (Integer) executeQuerySingleResult(
                "select lo(a, 0) from RASTEST1");
        assertEquals(-9999, lo.intValue());
    }
    
    @Test
    public void testLoName() throws SQLException {
        Integer lo = (Integer) executeQuerySingleResult(
                "select lo(a, 'd0') from RASTEST1");
        assertEquals(-9999, lo.intValue());
    }
    
    @Test
    public void testLoInvalidName() throws SQLException {
        try {
            Integer lo = (Integer) executeQuerySingleResult(
                    "select lo(a, 'x') from RASTEST1");
            fail();
        } catch (Exception ex) {
            assertEquals("The parameter passed to this function is invalid: Dimension name not found: x", ex.getMessage());
        }
    }
    
    @Test
    public void testHiIndex() throws SQLException {
        Integer hi = (Integer) executeQuerySingleResult(
                "select hi(a[y(100:110)], 1) from RASTEST2");
        assertEquals(110, hi.intValue());
    }
    
    @Test
    public void testHiIndex2() throws SQLException {
        Integer hi = (Integer) executeQuerySingleResult(
                "select hi(a[y(100:110)], 1) + lo(a, 0) from RASTEST2");
        assertEquals(110, hi.intValue());
    }
    
    @Test
    public void testHiName() throws SQLException {
        Integer hi = (Integer) executeQuerySingleResult(
                "select hi(a[y(100:110)], 'y') from RASTEST2");
        assertEquals(110, hi.intValue());
    }
    
    @Test
    public void testName() throws SQLException {
        String name = (String) executeQuerySingleResult(
                "select name(a[y(100:110)], 0) from RASTEST2");
        assertEquals("X", name);
    }
    
    @Test
    public void testDimensionality1() throws SQLException {
        Integer hi = (Integer) executeQuerySingleResult(
                "select dimensionality(a[y(100)]) from RASTEST2");
        assertEquals(1, hi.intValue());
    }
    
    @Test
    public void testDimensionality2() throws SQLException {
        Integer hi = (Integer) executeQuerySingleResult(
                "select dimensionality(a[y(100:110)]) from RASTEST2");
        assertEquals(2, hi.intValue());
    }
    
    /**
     * Test shift, scale, etc.
     */
    
    @Test
    public void testScale1() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select MDARRAY_SCALE(a+3, [0:100,0:100]) from RASTEST2");
        int[] d = res.getIntArray();
        assertEquals(10201, d.length);
    }
    
    @Test
    public void testScale2() throws SQLException {
        RasMArrayInteger res = (RasMArrayInteger) executeQuerySingleResult(
                "select MDARRAY_SCALE(a+3, [x(0:10), y(0:10)]) from RASTEST2");
        int[] d = res.getIntArray();
        assertEquals(121, d.length);
    }
    
    @Test
    public void testShift1() throws SQLException {
        Integer lo = (Integer) executeQuerySingleResult(
                "select lo(shift(a, [5,5]), 0) from RASTEST2");
        assertEquals(5, lo.intValue());
    }
    
    @Test
    public void testShift2() throws SQLException {
        RasMArrayByte res = (RasMArrayByte) executeQuerySingleResult(
                "select shift(a, [5,5]) from RASTEST2");
        assertEquals(0, res.getArray()[0]);
    }
    
    /**
     * Fails due to a rasdaman bug: http://rasdaman.org/ticket/858
     */
    @Ignore
    @Test
    public void testShift3() throws SQLException {
        RasMArrayDouble res = (RasMArrayDouble) executeQuerySingleResult(
                "select shift(a, [5]) from RASTEST2");
        assertEquals(1, res.getDoubleArray()[0], 0.01);
    }
    
    @Test
    public void testExtend1() throws SQLException {
        RasMArrayByte res = (RasMArrayByte) executeQuerySingleResult(
                "select extend(a, [0:300,0:300]) from RASTEST2");
        byte[] d = res.getArray();
        assertEquals(90601, d.length);
    }
    
    @Test
    public void testOverlay() throws SQLException {
        RasMArrayByte res = (RasMArrayByte) executeQuerySingleResult(
                "select a overlay cast(mdarray [x(0:255),y(0:210)] values 5 AS char) from RASTEST2");
        byte[] d = res.getArray();
        assertEquals(5, d[0]);
    }
    
}
