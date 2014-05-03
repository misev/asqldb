package org.hsqldb;

import org.hsqldb.error.*;
import org.hsqldb.error.Error;
import org.hsqldb.ras.ExpressionRas;

/**
 * Created by Johannes on 5/1/14.
 *
 * @author Johannes Bachhuber
 */
public class ExpressionRasElementList extends Expression implements ExpressionRas {

    public ExpressionRasElementList(final int type, final Expression... expressions) {
        super(type);
        nodes = expressions;

        switch (opType) {

            case OpTypes.ARRAY_DIMENSION_LIST:
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
        if (opType != OpTypes.ARRAY_DIMENSION_LIST) {
            throw new UnsupportedOperationException("This ElementList does not support this operation.");
        }
        for (Expression node : nodes) {
            if (((ExpressionRasDimensionLiteral) node).getName().equals(token.tokenString)) {
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
        if (opType != OpTypes.ARRAY_DIMENSION_LIST) {
            throw new UnsupportedOperationException("This ElementList does not support this operation.");
        }
        for (int i = 0, nodesLength = nodes.length; i < nodesLength; i++) {
            Expression node = nodes[i];
            if (((ExpressionRasDimensionLiteral) node).getName().equals(token.tokenString)) {
                return i;
            }
        }
        throw Error.error(ErrorCode.RAS_ARRAY_DIMENSION_REQUIRED);
    }
}
