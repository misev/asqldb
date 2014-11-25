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

import org.hsqldb.HsqlException;
import org.hsqldb.error.ErrorCode;

import java.util.Iterator;
import java.util.Set;

/**
 * Class to encapsulate information required to access rasdaman arrays.
 * In order to query the arrays stored in rasdaman, we need the name of 
 * the rasdaman collection the array is stored in,
 * and the oid of that array. We also need to assign it a new name within hsqldb,
 * which should be the name of the column containing the aid string.
 *
 * @author Dimitar Misev
 * @author Johannes Bachhuber
 */
public class RasArrayId {

    public static final char COLL_OID_SEPARATOR = ':';

    private int oid;
    private String coll;
    private String hsqlColName;


    private RasArrayId(String coll, int oid, String hsqlColName) {
        this.oid = oid;
        this.coll = coll;
        this.hsqlColName = hsqlColName;
    }

    /**
     * Parses a String of formation RASCOLLECTION:RASOID and makes the collection and oid accessible.
     * @param coid The formatted RasArrayId string.
     * @return new instance of RasArrayId
     * @throws HsqlException
     */
    public static RasArrayId parseString(String coid, String hsqlColName) throws HsqlException {
        final int idx = coid.indexOf(COLL_OID_SEPARATOR);
        if (idx == -1 //no COLL_OID_SEPARATOR found
                || coid.length() < 3 // COID too short
                || idx == 0 // Separator at the beginning
                || idx == coid.length()-1 ) { //separator at the end
            throw org.hsqldb.error.Error.error(ErrorCode.RAS_INVALID_COLL_OID, coid);
        }
        final String coll = coid.substring(0, idx);
        final Integer oid = Integer.parseInt(coid.substring(idx + 1));
        return new RasArrayId(coll, oid, hsqlColName);
    }

    /**
     * Helper method to convert any amount of RasArrayIds to a string to be used in the rasql from statement.
     * @param rasArrayIds Set of RasArrayIds
     * @return rasql FROM string
     */
    public static String stringifyRasCollections(Set<RasArrayId> rasArrayIds) {
        StringBuilder sb = new StringBuilder();
        for (Iterator<RasArrayId> iterator = rasArrayIds.iterator(); iterator.hasNext(); ) {
            RasArrayId coid = iterator.next();
            sb.append(coid.getCollection()).append(" as ").append(coid.getHsqlColName());
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * Helper method to convert RasArrayIds to a string to be used in the rasql where clause.
     * @param rasArrayIds Set of RasArrayIds
     * @return rasql where selector
     */
    public static String stringifyOids(Set<RasArrayId> rasArrayIds) {
        //todo: do we always want and?
        StringBuilder sb = new StringBuilder();
        for (Iterator<RasArrayId> iterator = rasArrayIds.iterator(); iterator.hasNext(); ) {
            RasArrayId coid = iterator.next();
            sb.append("oid(").append(coid.getHsqlColName()).append(") = ").append(coid.getOID());
            if (iterator.hasNext()) {
                sb.append(" and ");
            }
        }
        return sb.toString();
    }

    /**
     * Constructs a string identifier to be used as file name.
     * @param rasArrayIds Set of RasArrayIds
     * @return string identifier
     */
    public static String stringifyIdentifier(Set<RasArrayId> rasArrayIds) {
        StringBuilder sb = new StringBuilder();
        for (RasArrayId arrayId : rasArrayIds) {
            sb.append(arrayId.getCollection()).append(COLL_OID_SEPARATOR).append(arrayId.getOID());
        }
        return sb.toString();
    }


    /**
     * Getter for the Rasdaman collection name
     * @return Name of the collection
     */
    public String getCollection() {
        return coll;
    }

    /**
     * Getter for the Rasdaman oid.
     * @return the array's oid
     */
    public int getOID() {
        return oid;
    }

    @Override
    public String toString() {
        return coll+COLL_OID_SEPARATOR+oid+"("+hsqlColName+")";
    }

    /**
     * Getter for the HSQL Column name
     * @return name of the respective hsql column
     */
    public String getHsqlColName() {
        return hsqlColName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (! (other instanceof RasArrayId))
            return false;
        RasArrayId otherCoid = (RasArrayId) other;
        return oid == otherCoid.oid
                && coll.equals(otherCoid.coll)
                && hsqlColName.equals(otherCoid.hsqlColName);
    }

    @Override
    public int hashCode() {
        int hash = 5 + oid;
        hash = hash * 37 + coll.hashCode();
        hash = hash * 31 + hsqlColName.hashCode();
        return hash;
    }
}
