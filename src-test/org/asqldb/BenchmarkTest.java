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
import org.asqldb.util.TimerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Benchmark queries, used for profiling.
 *
 * @author Dimitar Misev
 */
public class BenchmarkTest extends BaseTest {
    
    protected static String[] createQueries;
    
    @BeforeClass
    public static void setUpData() {
        TimerUtil.startTimer("testPredefinedAggregation_AvgCells");
//        createQueries = insertTestData();
//        executeUpdateQuery("commit;", null);
    }
    
    @AfterClass
    public static void tearDownData() {
        //dropTables(createQueries);
    }
    
    @Test
    public void testPredefinedAggregation_AvgCells() throws SQLException {
        TimerUtil.printAllTimers();

        TimerUtil.resetTimer("RasUtil.executeRasqlQuery");
        executeQuerySingleResult(
                "select avg_cells(c.a) from A as c");
        TimerUtil.printAllTimers();

        TimerUtil.clearTimers();
        RasUtil.head(RasUtil.executeRasqlQuery(
                "select avg_cells(c) from PUBLIC_A_A as c", false));
        TimerUtil.printAllTimers();
    }
}
