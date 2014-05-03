package org.hsqldb;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.types.Type;

/**
 * Created by Johannes on 4/16/14.
 * @author Johannes Bachhuber
 */
public class ExpressionRasIndex extends Expression implements ExpressionRas {

    ExpressionRasIndex(int type, Expression left, Expression right) {
        super(type);
        nodes = new Expression[BINARY];
        nodes[LEFT] = left;
        nodes[RIGHT] = right;

        switch (opType) {

            case OpTypes.ARRAY_INDEX_LIST:
            case OpTypes.ARRAY_RANGE:
                break;

            default :
                throw org.hsqldb.error.Error.runtimeError(ErrorCode.U_S0500, "ExpressionRasIndex");
        }
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {
        for (Expression node : nodes) {
            if (node != null) {
                node.resolveTypes(session, this);
            }
        }
        dataType = Type.SQL_VARCHAR;
    }


    @Override
    public Object getValue(Session session, boolean isRasRoot) {

        switch(opType) {
            case OpTypes.ARRAY_RANGE:
                return nodes[LEFT].getValue(session, false)+":"+nodes[RIGHT].getValue(session, false);
            case OpTypes.ARRAY_INDEX_LIST:
                return nodes[LEFT].getValue(session, false)+","+nodes[RIGHT].getValue(session, false);
        }
        return null;
    }
}
