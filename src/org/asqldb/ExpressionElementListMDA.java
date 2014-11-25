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

import org.hsqldb.error.*;
import org.hsqldb.error.Error;
import org.hsqldb.Expression;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.Token;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionElementListMDA extends Expression implements ExpressionMDA {

    public ExpressionElementListMDA(final int type, final Expression... expressions) {
        super(type);
        nodes = expressions;

        switch (opType) {

            case OpTypes.ARRAY_DIMENSION_LIST:
            case OpTypes.ARRAY_DIMENSION_SDOM:
            case OpTypes.ARRAY_ELEMENT_LIST:
                break;

            default :
                throw org.hsqldb.error.Error.runtimeError(ErrorCode.U_S0500, "ExpressionRasIndex");
        }
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        for (Expression node : nodes) {
            if (node != null) {
                node.resolveTypes(session, this);
            }
        }
        //todo: type resolution necessary?
    }

    @Override
    public Object getValue(final Session session, final boolean isRasRoot) {
        if (isRasRoot) {
            throw new IllegalArgumentException("An ElementList has to be the child of an ArrayConstructor" +
                    " and can't be the RasRoot.");
        }

        if (opType == OpTypes.ARRAY_DIMENSION_SDOM) {
            return nodes[0].getValue(session, false);
        }

        final StringBuilder sb = new StringBuilder();

        if (opType == OpTypes.ARRAY_DIMENSION_LIST) {
            sb.append('[');
        }

        for (int i = 0, nodesLength = nodes.length; i < nodesLength; i++) {
            final Expression node = nodes[i];
            sb.append(node.getValue(session, false));
            if (i < nodesLength-1) {
                sb.append(", ");
            }
        }

        if (opType == OpTypes.ARRAY_DIMENSION_LIST) {
            sb.append(']');
        }
        return sb.toString();
    }

    /**
     * Determines whether the given token contains a reference to a dimension in this list.
     * @param token The token to be tested
     * @return true if the token is a reference to a dimension in this list, false otherwise
     */
    public boolean isDimensionName(final Token token) {
        switch(opType){
            case OpTypes.ARRAY_DIMENSION_SDOM:
                return token.tokenString.matches("(?i)d\\d+");
            case OpTypes.ARRAY_DIMENSION_LIST:
                break;
            default:
                throw new UnsupportedOperationException("This ElementList does not support this operation.");
        }
        for (Expression node : nodes) {
            if (((ExpressionDimensionLiteralMDA) node).getName().equals(token.tokenString)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieves the index of a dimension variable within this list.
     * @param token Token containing the variable name
     * @return the index of the given dimension
     */
    public int getIndexForName(final Token token) {
        switch(opType){
            case OpTypes.ARRAY_DIMENSION_SDOM:
                if (token.tokenString.matches("(?i)d\\d+")) {
                    //dX is the shortcut for dimensions when sdom(array) is being used.
                    //Should  the dimensions not match, rasdaman will throw an error
                    return Integer.parseInt(token.tokenString.substring(1));
                }
            case OpTypes.ARRAY_DIMENSION_LIST:
                break;
            default:
                throw new UnsupportedOperationException("This ElementList does not support this operation.");
        }
        for (int i = 0, nodesLength = nodes.length; i < nodesLength; i++) {
            Expression node = nodes[i];
            if (((ExpressionDimensionLiteralMDA) node).getName().equals(token.tokenString)) {
                return i;
            }
        }
        throw Error.error(ErrorCode.RAS_ARRAY_DIMENSION_REQUIRED);
    }
}
