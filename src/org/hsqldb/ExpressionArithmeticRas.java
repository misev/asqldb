package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.ras.RasArrayId;
import org.hsqldb.ras.RasUtil;

import java.util.Set;

/**
 * Created by Johannes on 4/17/14.
 * @author Johannes Bachhuber
 */
public class ExpressionArithmeticRas extends ExpressionArithmetic
                                     implements ExpressionRas {

    ExpressionArithmeticRas(int type, Expression left, Expression right) {

        super(type, left, right);
    }

    ExpressionArithmeticRas(int type, Expression e) {
        super(type, e);
    }

    @Override
    public Object getValue(Session session, boolean isRoot) {
        if (!nodes[LEFT].isArrayExpression() && !nodes[RIGHT].isArrayExpression()) {
            return super.getValue(session, isRoot);
        }
        switch (opType) {
            case OpTypes.VALUE :
                return valueData;

            case OpTypes.SIMPLE_COLUMN : {

                return session.sessionContext.rangeIterators[rangePosition]
                        .getCurrent(columnIndex);
            }
            case OpTypes.NEGATE :
                return dataType.negate(
                        nodes[LEFT].getValue(session, nodes[LEFT].dataType));
        }

        String operator;

        switch (opType) {
            case OpTypes.ADD:
                operator = "+";
                break;
            case OpTypes.SUBTRACT :
                operator = "-";
                break;
            case OpTypes.MULTIPLY :
                operator = "*";
                break;
            case OpTypes.DIVIDE :
                operator = "/";
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionRas");
        }
        Set<RasArrayId> rasArrayIds = nodes[LEFT].extractRasArrayIds(session);
        rasArrayIds.addAll(nodes[RIGHT].extractRasArrayIds(session));

        String selector = nodes[LEFT].getValue(session, false)+" "+operator
                +" "+nodes[RIGHT].getValue(session, false);

        if (isRoot) {//we're root, so we will execute the query
            return RasUtil.executeHsqlArrayQuery(selector, rasArrayIds);
        }
        //someone else will be executing the query, so we just return a rasql string
        //we only need to evaluate the hsql parts
        return selector;
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {
        for (Expression node : nodes) {
            if (node != null) {
                node.resolveTypes(session, this);
            }
        }
        if (!nodes[LEFT].isArrayExpression() && !nodes[RIGHT].isArrayExpression()) {
            super.resolveTypes(session, parent);
        }
    }

}
