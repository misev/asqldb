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

import org.hsqldb.Expression;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionDimensionLiteralMDA extends Expression implements ExpressionMDA {

    private HsqlNameManager.SimpleName name;
    private final int index;

    public ExpressionDimensionLiteralMDA(final HsqlNameManager.SimpleName name, final int index, final Expression range) {
        super(OpTypes.ARRAY_DIMENSION);
        this.name = name;
        this.index = index;
        nodes = new Expression[UNARY];
        nodes[LEFT] = range;
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        if (nodes[LEFT] != null) {
            nodes[LEFT].resolveTypes(session, this);
        }
        dataType = new ArrayType(Type.SQL_VARCHAR, 1);
    }

    @Override
    public Object getValue(Session session, boolean isRasRoot) {
        if (isRasRoot) {
            throw new IllegalArgumentException("An ElementList has to be the child of an ExpressionRasElementList"
                    +" and can't be the RasRoot.");
        }
        return nodes[0].getValue(session, false);
    }

    public String getName() {
        return name.getStatementName();
    }

    public int getIndex() {
        return index;
    }
}
