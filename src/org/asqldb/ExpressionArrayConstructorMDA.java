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

import org.hsqldb.error.ErrorCode;
import org.asqldb.ras.RasUtil;
import org.hsqldb.types.Type;

import org.asqldb.ras.RasArrayIdSet;
import org.hsqldb.Expression;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionArrayConstructorMDA extends Expression implements ExpressionMDA {

    private Object resultCache = null;

    public ExpressionArrayConstructorMDA(final int type, final Expression domain, final Expression values) {
        super(type);
        nodes = new Expression[BINARY];
        nodes[LEFT] = domain;
        nodes[RIGHT] = values;

        if (domain.opType != OpTypes.ARRAY_DIMENSION_LIST) {
            throw new IllegalArgumentException("Left operands must be of OpType ARRAY_DIMENSION_LIST");
        }

        switch (opType) {

            case OpTypes.ARRAY_CONSTRUCTOR_LITERAL:
            case OpTypes.ARRAY_CONSTRUCTOR_VALUE:
                break;

            default :
                throw org.hsqldb.error.Error.runtimeError(ErrorCode.U_S0500, "ExpressionRasIndex (type = "+opType+")");
        }
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        for (Expression node : nodes) {
            if (node != null) {
                node.resolveTypes(session, this);
            }
        }
        dataType = Type.SQL_MDARRAY_ALL_TYPES;
    }

    @Override
    public Object getValue(final Session session, final boolean isMDARootNode) {
        final String rasql;
        
        if (insertColumn == null) {
            switch (opType) {
                case OpTypes.ARRAY_CONSTRUCTOR_LITERAL:
                    rasql = String.format("< %s %s >", nodes[LEFT].getValue(session, false), nodes[RIGHT].getValue(session, false));
                    break;
                case OpTypes.ARRAY_CONSTRUCTOR_VALUE:
                    rasql = String.format("(marray x in %s values %s)", nodes[LEFT].getValue(session, false), nodes[RIGHT].getValue(session, false));
                    break;

                default:
                    throw org.hsqldb.error.Error.runtimeError(ErrorCode.U_S0500, "ExpressionRasIndex (type = "+opType+")");
            }
            if (isMDARootNode) {
                // Cache the result, since it won't change for other rows
                final RasArrayIdSet rasArrayIds = getRasArrayIds(session);
                if (!rasArrayIds.isEmpty()) {
                    return RasUtil.executeHsqlArrayQuery(rasql, rasArrayIds);
                }
                if (resultCache == null) {
                    resultCache = RasUtil.executeHsqlArrayQuery(rasql, rasArrayIds);
                }
                return resultCache;
            }
            
        } else {
            
            String collName = insertColumn.getRasdamanCollectionName();
            Type type = insertColumn.dataType.collectionBaseType();
            String left = nodes[LEFT].getValue(session, false).toString();
            String right = nodes[RIGHT].getValue(session, false).toString();
            String insertQuery = "INSERT INTO " + collName + " VALUES ";
            
            if (opType == OpTypes.ARRAY_CONSTRUCTOR_LITERAL) {
                String suffix = type.getRasqlSuffix();
                right = right.replaceAll("(\\-?\\d+\\.\\d+)", "$1" + suffix);
                insertQuery += "< " + left + " " + right + " >";
                
            } else if (opType == OpTypes.ARRAY_CONSTRUCTOR_VALUE) {
                ExpressionElementListMDA el = (ExpressionElementListMDA) nodes[LEFT];
                Expression[] dims = el.getNodes();
                
                insertQuery += "MARRAY x IN [";
                for (int i = 0; i < dims.length; i++) {
                    ExpressionDimensionLiteralMDA dim = (ExpressionDimensionLiteralMDA) dims[i];
                    
                    // append range
                    final String range = dim.getNodes()[0].getValue(session, false).toString();
                    insertQuery += range;
                    if (i < dims.length - 1) {
                        insertQuery += ", ";
                    }
                    
                    // replace dimension name references with rasql equivalents in the values clause
                    final String name = dim.getName();
                    final String replName = "x[" + i + "]";
                    right = right.replaceAll(name, replName);
                }
                insertQuery += "] VALUES " + right;
            }
            if (resultCache == null) {
                final Object result = RasUtil.executeRasqlQuery(insertQuery, false, true);
                resultCache = RasUtil.dbagToOid(result);
            }
            return resultCache;
        }

        return rasql;
    }

}
