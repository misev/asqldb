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

import java.util.Iterator;
import org.asqldb.ras.RasUtil;
import org.odmg.DBag;
import rasj.RasMArrayByte;

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
public class InsertDeleteTest extends CreateTest {

    public static void main(String[] args) {
        connect();
        
        final String[] createQueries = new String[]{
            "create table RASTEST1 ("
                + "a DOUBLE MDARRAY[-10000:-1000])"};
        
        dropTables(createQueries);
//        createTables(createQueries);
//        insertArrayLiteral();
//        checkInsertedArrayLiteral();
//        dropTables(createQueries);
        
        createTables(createQueries);
        insertArrayValues();
        checkInsertedArrayValues();
        dropTables(createQueries);
        
        disconnect();
        
//        System.exit(failed);
    }
    
    public static int insertArrayLiteral() {
        System.out.println("\nInserting array literal...");
        final String[] insertQueries = new String[]{
            "insert into RASTEST1(a) values ("
                + "MDARRAY[-9999:-9997] [1.0,2.3,-9.88832])"};
        return executeQueries(insertQueries);
    }
    
    public static int insertArrayValues() {
        System.out.println("\nInserting array values...");
        final String[] insertQueries = new String[]{
            "insert into RASTEST1(a) values ("
                + "MDARRAY[x(-9999:-9997)] VALUES CAST(x AS DOUBLE))"};
        return executeQueries(insertQueries);
    }
    
    public static boolean checkInsertedArrayLiteral() {
        System.out.print("Checking inserted data... ");
        boolean ret = true;
        Object res = RasUtil.executeRasqlQuery("select csv(c) from PUBLIC_RASTEST1_A as c", true);
        if (res instanceof DBag) {
            DBag b = (DBag) res;
            Iterator it = b.iterator();
            Object o = it.next();
            if (o instanceof RasMArrayByte) {
                RasMArrayByte m = (RasMArrayByte) o;
                String csv = new String(m.getArray());
                ret = csv.equals("{1,2.3,-9.88832}");
            } else {
                ret = false;
            }
        } else {
            ret = false;
        }
        printCheck(ret);
        return ret;
    }
    
    public static boolean checkInsertedArrayValues() {
        System.out.print("Checking inserted data... ");
        boolean ret = true;
        Object res = RasUtil.executeRasqlQuery("select csv(c) from PUBLIC_RASTEST1_A as c", true);
        if (res instanceof DBag) {
            DBag b = (DBag) res;
            Iterator it = b.iterator();
            Object o = it.next();
            if (o instanceof RasMArrayByte) {
                RasMArrayByte m = (RasMArrayByte) o;
                String csv = new String(m.getArray());
                ret = csv.equals("{-9999,-9998,-9997}");
            } else {
                ret = false;
            }
        } else {
            ret = false;
        }
        printCheck(ret);
        return ret;
    }
}
