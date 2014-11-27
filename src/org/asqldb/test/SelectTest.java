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

package org.asqldb.test;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.asqldb.ras.RasUtil;
import static org.asqldb.test.BaseTest.connection;
import rasj.RasMArrayDouble;

/**
 * Run SQL/MDA select tests.
 *
 * @author Dimitar Misev
 */
public class SelectTest extends InsertDeleteTest {

    public static void main(String[] args) {
        boolean result = true;
        int exitValue = 0;
        
        connect();
        
        final String[] createQueries = insertTestData();
        
        result = result && testSimpleSelect();
        dropTables(createQueries);
        
        disconnect();
        
        exitValue += (result ? 0 : 1);
        System.exit(exitValue);
    }
    
    public static boolean testSimpleSelect() {
        System.out.print("\nTest pure array select...");
        boolean ret = true;
        
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("select c.a from RASTEST1 as c");
            while (rs.next()) {
                Object o = RasUtil.head(rs.getObject(1));
                RasMArrayDouble ma = (RasMArrayDouble) o;
                double[] d = ma.getDoubleArray();
                ret = ret && d.length == 3;
                printCheck(ret);
            }
        } catch (SQLException e) {
            System.out.println(" ... failed.");
            e.printStackTrace();
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
        }
        
        return ret;
    }
}
