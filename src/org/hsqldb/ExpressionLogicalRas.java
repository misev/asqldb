package org.hsqldb;

import org.hsqldb.error.*;
import org.hsqldb.ras.RasArrayId;
import org.hsqldb.ras.RasUtil;
import org.hsqldb.types.Type;

import java.util.Set;

/**
 * Created by johannes on 5/25/14.
 */
public class ExpressionLogicalRas extends ExpressionLogical {
    ExpressionLogicalRas(int type, Expression left, Expression right) {
        super(type, left, right);
    }

    ExpressionLogicalRas(int type, Expression e) {
        super(type, e);
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        if ((nodes[LEFT] == null || !nodes[LEFT].isArrayExpression())
                && (nodes.length < 2 || nodes[RIGHT] == null || !nodes[RIGHT].isArrayExpression())) {
            super.resolveTypes(session, parent);
            return;
        }
        dataType = Type.SQL_BOOLEAN;
    }

    @Override
    public Object getValue(Session session, boolean isRoot) {
        if (!nodes[LEFT].isArrayExpression() && !nodes[RIGHT].isArrayExpression()) {
            return super.getValue(session, isRoot);
        }

        String operator;

        switch (opType) {
            case OpTypes.EQUAL :
                operator = "=";
                break;
            case OpTypes.GREATER_EQUAL :
                operator = ">=";
                break;
            case OpTypes.GREATER :
                operator = ">";
                break;
            case OpTypes.SMALLER :
                operator = "<";
                break;
            case OpTypes.SMALLER_EQUAL :
                operator = "<=";
                break;
            case OpTypes.NOT_EQUAL :
                operator = "!=";
                break;
            case OpTypes.AND :
                operator = "and";
                break;
            case OpTypes.OR :
                operator = "or";
                break;
            default :
                throw org.hsqldb.error.Error.runtimeError(ErrorCode.U_S0500, "ExpressionRas");
        }
        Set<RasArrayId> rasArrayIds = nodes[LEFT].extractRasArrayIds(session);
        rasArrayIds.addAll(nodes[RIGHT].extractRasArrayIds(session));

        String selector = nodes[LEFT].getValue(session, false)+" "+operator
                +" "+nodes[RIGHT].getValue(session, false);

        if (isRoot) {//we're root, so we will execute the query
            return Boolean.valueOf(RasUtil.executeHsqlArrayQuery(selector, rasArrayIds));
        }
        //someone else will be executing the query, so we just return a rasql string
        //we only need to evaluate the hsql parts
        return selector;
    }
}
