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

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Execute CREATE tests.
 * 
 * @TODO: test error cases
 *
 * @author Dimitar Misev
 */
public class CreateDropTest extends BaseTest {
        
    final String[] createQueries = new String[]{
        "create table RASTEST1 ("
            + "id INTEGER NOT NULL,"
            + "a INTEGER MDARRAY[x,y],"
            + "PRIMARY KEY (id))",
        "create table RASTEST2 ("
            + "a INTEGER MDARRAY[x(0:1000),y(0:1000)])",
        "create table RASTEST3 ("
            + "a DOUBLE MDARRAY[0:1000,0:1000])",
        "create table RASTEST4 ("
            + "a DOUBLE MDARRAY[0:1000,-100:1000,z(1:100)])",
        "create table RASTEST5 ("
            + "a DOUBLE MDARRAY[-10000:-1000,-100:1000,z(-1000:-100)])",
        "create table RASTEST6 ("
            + "a DOUBLE MDARRAY[-10000:-1000])"};
    
    @Test
    public void testCreateDrop() {
        dropTables(createQueries);
        
        assertEquals(createQueries.length, createTables(createQueries));
        
        for (int i = 1; i <= createQueries.length; i++) {
            final String table = "RASTEST" + i;
            assertTrue(tableExistsInRasdaman(table));
        }
        
        assertTrue(dropTables(createQueries));
    }
}
