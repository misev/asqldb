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
package org.asqldb.ras;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maintain a catalog of rasdaman collections, to avoid unnecessary rasql check
 * queries.
 *
 * @author Dimitar Misev
 */
public class RasCollCatalog {

    private static final Set<String> colls = new HashSet<String>();
    public static boolean initialized = false;

    private static final String GET_COLLS_QUERY = "select c from RAS_COLLECTIONNAMES as c";

    public static final Pattern CREATE_COLL_PATERN = Pattern.compile("create\\s+collection\\s+([^\\s]+).*");
    public static final Pattern DROP_COLL_PATERN = Pattern.compile("drop\\s+collection\\s+(.+)");

    public static void init() {
        if (!initialized) {
            update();
            initialized = true;
        }
    }

    public static void update() {
        Set<String> currColls = RasUtil.dbagArrayToSetString(
                RasUtil.executeRasqlQuery(GET_COLLS_QUERY, true));
        clear();
        colls.addAll(currColls);
    }

    public static boolean contains(String coll) {
        if (coll != null) {
            return colls.contains(coll);
        } else {
            return false;
        }
    }

    public static void add(String coll) {
        if (coll != null) {
            colls.add(coll);
        }
    }

    public static void remove(String coll) {
        if (coll != null) {
            colls.remove(coll);
        }
    }

    public static void clear() {
        colls.clear();
    }

    public static void update(String query) {
        String normQuery = query.toLowerCase().trim();
        if (normQuery.startsWith("create ")) {
            RasCollCatalog.add(getName(normQuery, CREATE_COLL_PATERN));
        } else if (normQuery.startsWith("drop ")) {
            RasCollCatalog.remove(getName(normQuery, DROP_COLL_PATERN));
        }
    }

    private static String getName(String query, Pattern pattern) {
        try {
            Matcher matcher = CREATE_COLL_PATERN.matcher(query);
            return matcher.group(1);
        } catch (Exception ex) {
            return null;
        }
    }
}
