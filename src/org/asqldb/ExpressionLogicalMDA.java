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
import org.asqldb.ras.RasArrayId;
import org.asqldb.ras.RasUtil;
import org.hsqldb.types.Type;

import java.util.Set;
import org.hsqldb.Expression;
import org.hsqldb.ExpressionLogical;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionLogicalMDA extends ExpressionLogical {
    public ExpressionLogicalMDA(int type, Expression left, Expression right) {
        super(type, left, right);
    }

    public ExpressionLogicalMDA(int type, Expression e) {
        super(type, e);
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        if ((nodes[LEFT] == null || !nodes[LEFT].isExpressionMDA())
                && (nodes.length < 2 || nodes[RIGHT] == null || !nodes[RIGHT].isExpressionMDA())) {
            super.resolveTypes(session, parent);
            return;
        }
        dataType = Type.SQL_BOOLEAN;
    }

    @Override
    public Object getValue(Session session, boolean isRoot) {
        if (nodes.length == 1 && !nodes[LEFT].isExpressionMDA()) {
            return super.getValue(session, isRoot);
        } else if (nodes.length == 2 && !nodes[LEFT].isExpressionMDA() && !nodes[RIGHT].isExpressionMDA()) {
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
        Set<RasArrayId> rasArrayIds = nodes[LEFT].getRasArrayIds(session);
        rasArrayIds.addAll(nodes[RIGHT].getRasArrayIds(session));

        String selector = nodes[LEFT].getValue(session, false)+" "+operator
                +" "+nodes[RIGHT].getValue(session, false);

        if (isRoot) {//we're root, so we will execute the query
            return Boolean.valueOf(RasUtil.executeHsqlArrayQuery(selector, rasArrayIds).toString());
        }
        //someone else will be executing the query, so we just return a rasql string
        //we only need to evaluate the hsql parts
        return selector;
    }
}
