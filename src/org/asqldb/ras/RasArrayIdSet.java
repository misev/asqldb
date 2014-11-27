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


import java.util.Collection;
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
public class RasArrayIdSet extends java.util.LinkedHashSet<RasArrayId> {

    public RasArrayIdSet() {
        super();
    }

    public RasArrayIdSet(Collection<? extends RasArrayId> clctn) {
        super(clctn);
    }

    /**
     * Helper method to convert RasArrayIds to a string to be used in the rasql where clause.
     * @return rasql where selector
     */
    public String stringifyOids() {
        StringBuilder sb = new StringBuilder();
        for (Iterator<RasArrayId> iterator = iterator(); iterator.hasNext(); ) {
            RasArrayId id = iterator.next();
            sb.append("oid(").append(id.getHsqlField()).append(") = ").append(id.getRasOid());
            if (iterator.hasNext()) {
                sb.append(" and ");
            }
        }
        return sb.toString();
    }

    /**
     * Helper method to convert RasArrayIds to a string to be used in the 
     * rasql FROM clause.
     * @return rasql FROM clause
     */
    public String stringifyRasColls() {
        StringBuilder sb = new StringBuilder();
        for (Iterator<RasArrayId> iterator = iterator(); iterator.hasNext(); ) {
            RasArrayId id = iterator.next();
            sb.append(id.getRasColl()).append(" AS ").append(id.getHsqlField());
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * Constructs a string identifier to be used as file name.
     * @return string identifier
     */
    public String stringifyIdentifier() {
        StringBuilder sb = new StringBuilder();
        for (RasArrayId id : this) {
            sb.append(id.getRasColl()).append("_").append(id.getRasOid());
        }
        return sb.toString();
    }
}
