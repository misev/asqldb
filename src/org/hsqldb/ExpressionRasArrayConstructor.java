package org.hsqldb;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.ras.RasArrayId;
import org.hsqldb.ras.RasUtil;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Johannes on 4/29/14.
 * @author Johannes Bachhuber
 */
public class ExpressionRasArrayConstructor extends Expression implements ExpressionRas {

    private Object resultCache = null;

    public ExpressionRasArrayConstructor(final int type, final Expression dimensions, final Expression values) {
        super(type);
        nodes = new Expression[BINARY];
        nodes[LEFT] = dimensions;
        nodes[RIGHT] = values;

        if (dimensions.opType != OpTypes.ARRAY_DIMENSION_LIST) {
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
        dataType = Type.SQL_VARCHAR;
    }

    @Override
    public Set<RasArrayId> extractRasArrayIds(final Session session) {
        return new HashSet<RasArrayId>();
    }

    @Override
    public Object getValue(final Session session, final boolean isRasRoot) {
        final String rasql;
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


        if (isRasRoot) {
            //Cache the result, since it won't change for other rows
            if (resultCache == null)
                resultCache = RasUtil.executeHsqlArrayQuery(rasql, new HashSet<RasArrayId>());
            return resultCache;
        }

        return rasql;
    }

}
