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

import org.asqldb.types.MDADimensionType;
import org.asqldb.types.MDADomainType;
import org.hsqldb.error.*;
import org.hsqldb.error.Error;
import org.hsqldb.Expression;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionElementListMDA extends Expression implements ExpressionMDA {

    public ExpressionElementListMDA(final int type, final Expression... expressions) {
        super(type);
        nodes = expressions;

        switch (opType) {
            case OpTypes.ARRAY_SUBSET_RANGE:
            case OpTypes.ARRAY_DOMAIN_DEFINITION:
            case OpTypes.ARRAY_LITERAL:
                break;

            default :
                throw org.hsqldb.error.Error.runtimeError(
                        ErrorCode.U_S0500, "ExpressionElementListMDA");
        }
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        resolveChildrenTypes(session);

        switch (opType) {
            case OpTypes.ARRAY_SUBSET_RANGE:
            case OpTypes.ARRAY_DOMAIN_DEFINITION:
                MDADomainType domainType = new MDADomainType();
                for (Expression dim : nodes) {
                    domainType.addDimension((MDADimensionType) dim.getDataType());
                }
                dataType = domainType;
                break;
            case OpTypes.ARRAY_LITERAL:
                Type cellType = null;
                if (nodes.length > 0) {
                    cellType = nodes[0].getDataType();
                }
                dataType = new ArrayType(cellType, nodes.length);
        }
    }

    @Override
    public Object getValue(final Session session, final boolean isMDARootNode) {
        return getValue(session, null, isMDARootNode);
    }

    @Override
    public Object getValue(final Session session, Type parentDomainType, final boolean isMDARootNode) {
        if (isMDARootNode) {
            throw new IllegalArgumentException("An ElementList has to be the child of an ArrayConstructor" +
                    " and can't be the RasRoot.");
        }

        final StringBuilder sb = new StringBuilder();

        if (opType == OpTypes.ARRAY_DOMAIN_DEFINITION || opType == OpTypes.ARRAY_SUBSET_RANGE) {
            sb.append('[');
        }

        boolean evaluatedChildrenNodes = false;
        if (opType == OpTypes.ARRAY_SUBSET_RANGE && parentDomainType != null) {
            MDADomainType subsetType = (MDADomainType) dataType;
            MDADomainType parentType = (MDADomainType) parentDomainType;
            if (subsetType.isNamedSubset(parentType)) {
                int dimensionality = parentType.getDimensionality();
                for (int i = 0; i < dimensionality; i++) {
                    MDADimensionType dimensionType = parentType.getDimension(i);
                    int ind = subsetType.getDimensionIndex(dimensionType.getDimensionName());
                    if (ind != -1) {
                        sb.append(nodes[ind].getValue(session, false));
                    } else {
                        sb.append("*:*");
                    }
                    if (i < dimensionality - 1) {
                        sb.append(',');
                    }
                }
                evaluatedChildrenNodes = true;
            }
        }
        
        if (!evaluatedChildrenNodes) {
            for (int i = 0, nodesLength = nodes.length; i < nodesLength; i++) {
                sb.append(nodes[i].getValue(session, false));
                if (i < nodesLength - 1) {
                    sb.append(", ");
                }
            }
        }

        if (opType == OpTypes.ARRAY_DOMAIN_DEFINITION || opType == OpTypes.ARRAY_SUBSET_RANGE) {
            sb.append(']');
        }
        return sb.toString();
    }
}
