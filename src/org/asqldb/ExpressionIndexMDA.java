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

import java.util.Objects;
import org.asqldb.types.MDADimensionType;
import org.hsqldb.Expression;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.types.Type;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionIndexMDA extends Expression implements ExpressionMDA {
    
    private HsqlNameManager.SimpleName name = null;
    private int index = -1;

    /**
     * Subset of the form: *:*
     */
    public ExpressionIndexMDA() {
        super(OpTypes.ARRAY_SUBSET_RANGE);
        nodes = new Expression[BINARY];
        nodes[LEFT] = new ExpressionIndexUnboundedMDA();
        nodes[RIGHT] = new ExpressionIndexUnboundedMDA();
    }

    /**
     * Slice on a named dimension, of the form dim(slice)
     */
    public ExpressionIndexMDA(Expression slice) {
        super(OpTypes.ARRAY_SUBSET_SLICE);
        nodes = new Expression[UNARY];
        nodes[LEFT] = slice;
    }

    /**
     * Subset on a named dimension, of the form dim(left:right)
     */
    public ExpressionIndexMDA(Expression left, Expression right) {
        super(OpTypes.ARRAY_SUBSET_RANGE);
        nodes = new Expression[BINARY];
        nodes[LEFT] = left;
        nodes[RIGHT] = right;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public HsqlNameManager.SimpleName getName() {
        return name;
    }

    public void setName(HsqlNameManager.SimpleName name) {
        this.name = name;
    }

    public String getNameString() {
        HsqlNameManager.SimpleName myname = getName();
        if (myname != null) {
            return myname.name;
        } else {
            return null;
        }
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {
        for (Expression node : nodes) {
            if (node != null) {
                node.resolveTypes(session, this);
                Type nodeDataType = node.getDataType();
                if (!nodeDataType.isNumberType() && !nodeDataType.isCharacterType()) {
                    throw org.hsqldb.error.Error.error(ErrorCode.MDA_INVALID_SUBSET,
                            "Invalid index type, expected a number expression or '*'.");
                }
            }
        }
        
        String valueLeft = nodes[LEFT].getValue(session, false).toString();
        if (nodes.length == 1) {
            dataType = new MDADimensionType(getNameString(), valueLeft);
        } else {
            String valueRight = nodes[RIGHT].getValue(session, false).toString();
            dataType = new MDADimensionType(getNameString(), valueLeft, valueRight);
        }
    }

    /**
     * @TODO: implement proper translation to rasql of named subsets
     */
    @Override
    public Object getValue(Session session, boolean isMDARootNode) {
        if (nodes.length == 1) {
            return nodes[LEFT].getValue(session, false);
        } else {
            return nodes[LEFT].getValue(session, false) + ":" + 
                    nodes[RIGHT].getValue(session, false);
        }
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + Objects.hashCode(this.name.name);
        hash = 53 * hash + this.index;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ExpressionIndexMDA other = (ExpressionIndexMDA) obj;
        if (!Objects.equals(this.name.name, other.name.name)) {
            return false;
        }
        if (this.index != other.index) {
            return false;
        }
        return true;
    }
    
    
}
