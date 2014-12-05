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

import org.asqldb.types.MDADomainType;
import org.hsqldb.Expression;
import org.hsqldb.lib.HsqlArrayList;

/**
 * Super class for expressions which involve some implicit iteration, like
 * general array constructor and aggregation.
 * 
 * @author Dimitar Misev
 */
public abstract class ExpressionIterationMDA extends Expression implements ExpressionMDA {
    
    public static final String RASQL_ITERATOR_NAME_PREFIX = "it";

    protected String rasqlIteratorName = null;

    public ExpressionIterationMDA(int type) {
        super(type);
    }
    
    protected void setRasqlIteratorNames() {
        if (rasqlIteratorName == null) {
            HsqlArrayList iterationNodes = getChildren(ExpressionIterationMDA.class);
            
            for (int i = 0, counter = 1; i < iterationNodes.size(); i++, counter++) {
                ExpressionIterationMDA iterationNode = (ExpressionIterationMDA) iterationNodes.get(i);
                MDADomainType domainType = getDomainType(iterationNode);
                if (domainType == null) {
                    continue;
                }
                
                String newRasqlIteratorName = newRasqlIteratorName(counter);
                iterationNode.setRasqlIteratorName(newRasqlIteratorName);
                
                HsqlArrayList iterationRefNodes = getChildren(ExpressionValueVariableMDA.class);
                for (int j = 0; j < iterationRefNodes.size(); j++) {
                    ExpressionValueVariableMDA iterationRefNode = (ExpressionValueVariableMDA) iterationRefNodes.get(j);
                    String name = iterationRefNode.getHsqlIteratorName();
                    if (domainType.hasDimension(name)) {
                        iterationRefNode.setRasqlIteratorName(newRasqlIteratorName);
                    }
                }
            }
        }
        if (rasqlIteratorName == null) {
            rasqlIteratorName = "x";
        }
    }
    
    private MDADomainType getDomainType(ExpressionIterationMDA node) {
        Expression[] childNodes = node.getNodes();
        if (childNodes.length > 0 && childNodes[0] instanceof ExpressionElementListMDA) {
            return (MDADomainType) childNodes[0].getDataType();
        } else {
            return null;
        }
    }
    
    public String newRasqlIteratorName(int level) {
        return RASQL_ITERATOR_NAME_PREFIX + level;
    }

    public void setRasqlIteratorName(String iteratorName) {
        this.rasqlIteratorName = iteratorName;
    }

}
