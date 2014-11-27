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

/**
 * Class to encapsulate information required to access rasdaman arrays. In order
 * to query the arrays stored in rasdaman, we need the name of the rasdaman
 * collection the array is stored in, and the oid of that array.
 *
 * @author Dimitar Misev
 * @author Johannes Bachhuber
 */
public class RasArrayId {

    public static final char COLL_OID_SEPARATOR = ':';

    private final Integer rasOid;
    private final String rasColl;
    private final String hsqlField;

    /**
     *
     * @param rasColl rasdaman collection name
     * (ColumnSchema.getRasdamanCollection())
     * @param rasOid array OID in rasdaman
     * @param hsqlField field name in the HSQL table
     */
    public RasArrayId(String rasColl, Integer rasOid, String hsqlField) {
        this.rasOid = rasOid;
        this.rasColl = rasColl;
        this.hsqlField = hsqlField;
    }

    /**
     * @return Name of the MDA (rasdaman) collection
     */
    public String getRasColl() {
        return rasColl;
    }

    /**
     * @return the array's oid in rasdaman
     */
    public int getRasOid() {
        return rasOid;
    }

    /**
     * @return name of the respective hsql column
     */
    public String getHsqlField() {
        return hsqlField;
    }

    @Override
    public String toString() {
        return rasColl + COLL_OID_SEPARATOR + rasOid + "(" + hsqlField + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RasArrayId)) {
            return false;
        }
        RasArrayId otherCoid = (RasArrayId) other;
        return rasOid.equals(otherCoid.rasOid)
                && rasColl.equals(otherCoid.rasColl);
    }

    @Override
    public int hashCode() {
        int hash = 5 + rasOid;
        hash = hash * 37 + rasColl.hashCode();
        return hash;
    }
}
