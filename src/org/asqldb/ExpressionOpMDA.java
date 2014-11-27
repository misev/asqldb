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

import org.asqldb.ras.RasArrayId;
import org.asqldb.ras.RasUtil;
import org.hsqldb.types.Type;

import java.util.Set;
import org.hsqldb.Expression;
import org.hsqldb.ExpressionOp;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;

/**
 * Covers MDA CAST expressions.
 * 
 * @author Dimitar Misev
 */
public class ExpressionOpMDA extends ExpressionOp {
    
    public ExpressionOpMDA(Expression e, Type dataType) {
        super(e, dataType);
    }

//    @Override
//    public void resolveTypes(final Session session, final Expression parent) {
//        if (opType == OpTypes.CAST && nodes[LEFT].isExpressionMDA()) {
//            nodes[LEFT].resolveTypes(session, parent);
//            
//            dataType = Type.SQL_BOOLEAN;
//        } else {
//            super.resolveTypes(session, parent);
//        }
//        
//    }

    @Override
    public Object getValue(Session session, boolean isRoot) {
        if (opType == OpTypes.CAST && nodes[LEFT].isExpressionMDA()) {
            String castType = dataType.getRasqlType();
            String castValue = nodes[LEFT].getValue(session, false).toString();
            String rasqlQuery = "((" + castType + ") " + castValue + ")";

            // this is the rasql root node, so it has to be executed
            if (isRoot) {
                Set<RasArrayId> rasArrayIds = nodes[LEFT].getRasArrayIds(session);
                return RasUtil.executeHsqlArrayQuery(rasqlQuery, rasArrayIds);
            }
            return rasqlQuery;
        } else {
            return super.getValue(session, isRoot);
        }
    }
}
