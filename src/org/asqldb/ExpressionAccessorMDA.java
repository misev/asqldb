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

import org.asqldb.ras.RasArrayId;
import org.asqldb.ras.RasUtil;
import org.hsqldb.types.Type;

import java.util.Set;
import org.hsqldb.Expression;
import org.hsqldb.ExpressionAccessor;
import org.hsqldb.Session;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionAccessorMDA extends ExpressionAccessor implements ExpressionMDA {

    public ExpressionAccessorMDA(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        if (nodes != null && nodes.length > 0) {
            nodes[LEFT].resolveTypes(session, this);
        }
        dataType = Type.SQL_VARCHAR;
    }

    @Override
    public Object getValue(Session session, boolean isMDARootNode) {
        if (nodes != null && nodes.length > 1) {
            final String index = (nodes[RIGHT] == null) ? "" :
                    ("[" + nodes[RIGHT].getValue(session, false) + "]");
            final String colName = nodes[LEFT].getColumnNameString();

            if (isMDARootNode) {
                return RasUtil.executeHsqlArrayQuery(colName + index, getRasArrayIds(session));
            }
            return colName + index;
        }
        return null;
    }
}
