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

import org.asqldb.ras.RasUtil;
import rasj.RasMArrayByte;
import rasj.RasMArrayDouble;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

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
    
    @Test
    public void testSingleArraySelect() {
        Object dbag = executeQuerySingleResult("select c.a from RASTEST1 as c");
        RasMArrayDouble res = (RasMArrayDouble) RasUtil.head(dbag);
        double[] d = res.getDoubleArray();
        assertEquals(d.length, 3);
    }
    
    @Test
    public void testSingleArrayEncode() {
        Object dbag = executeQuerySingleResult("select mdarray_encode(c.a, 'PNG') from RASTEST2 as c");
        RasMArrayByte res = (RasMArrayByte) RasUtil.head(dbag);
        byte[] d = res.getArray();
        assertEquals(d.length, 22624);
    }
    
    @Test
    public void testPredefinedAggregation_AvgCells() {
        Double hsqlRes = (Double) executeQuerySingleResult(
                "select avg_cells(c.a) from RASTEST2 as c");
        Double rasqlRes = (Double) RasUtil.head(RasUtil.executeRasqlQuery(
                "select avg_cells(c) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes, hsqlRes);
    }
    
    @Test
    public void testPredefinedAggregation_AddCells() {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select add_cells(c.a = 0) from RASTEST2 as c");
        Long rasqlRes = (Long) RasUtil.head(RasUtil.executeRasqlQuery(
                "select add_cells(c = 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes.longValue(), hsqlRes.longValue());
    }
    
    @Test
    public void testPredefinedAggregation_CountCells() {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select count_cells(c.a != 0) from RASTEST2 as c");
        Long rasqlRes = (Long) RasUtil.head(RasUtil.executeRasqlQuery(
                "select count_cells(c != 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes.longValue(), hsqlRes.longValue());
    }
    
    @Test
    public void testPredefinedAggregation_SomeCells() {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select some_cells(c.a > 0) from RASTEST2 as c");
        Integer rasqlRes = (Integer) RasUtil.head(RasUtil.executeRasqlQuery(
                "select some_cells(c > 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes > 0);
        assertEquals(rasqlRes, hsqlRes);
    }
    
    @Test
    public void testPredefinedAggregation_AllCells() {
        Integer hsqlRes = (Integer) executeQuerySingleResult(
                "select all_cells(c.a <= 0) from RASTEST2 as c");
        Integer rasqlRes = (Integer) RasUtil.head(RasUtil.executeRasqlQuery(
                "select all_cells(c <= 0) from PUBLIC_RASTEST2_A as c", false));
        assertTrue(hsqlRes == 0);
        assertEquals(rasqlRes, hsqlRes);
    }
}
