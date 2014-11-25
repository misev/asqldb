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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.asqldb.ras.RasArrayId;
import org.asqldb.ras.RasUtil;

import java.util.Set;
import org.hsqldb.Expression;
import org.hsqldb.ExpressionArithmetic;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionArithmeticMDA extends ExpressionArithmetic {

    public ExpressionArithmeticMDA(int type, Expression left, Expression right) {

        super(type, left, right);
    }

    public ExpressionArithmeticMDA(int type, Expression e) {
        super(type, e);
    }

    @Override
    public Object getValue(Session session, boolean isRoot) {
        if (nodes.length == 0 ||
                (!nodes[LEFT].isExpressionMDA() &&
                        (nodes.length < 2 || !nodes[RIGHT].isExpressionMDA()))) {
            return super.getValue(session, isRoot);
        }
        switch (opType) {
            case OpTypes.VALUE :
                return valueData;

            case OpTypes.SIMPLE_COLUMN : {

                return session.sessionContext.getRangeIterators()[rangePosition]
                        .getCurrent(getColumnIndex());
            }
            case OpTypes.NEGATE :
                return dataType.negate(
                        nodes[LEFT].getValue(session, nodes[LEFT].getDataType()));
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
        Set<RasArrayId> rasArrayIds = nodes[LEFT].getRasArrayIds(session);
        rasArrayIds.addAll(nodes[RIGHT].getRasArrayIds(session));

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
        if (!nodes[LEFT].isExpressionMDA() && !nodes[RIGHT].isExpressionMDA()) {
            super.resolveTypes(session, parent);
        }
    }

}
