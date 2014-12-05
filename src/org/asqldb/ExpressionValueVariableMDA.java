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

import org.hsqldb.error.ErrorCode;
import org.hsqldb.Expression;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.types.Type;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionValueVariableMDA extends Expression implements ExpressionMDA {

    private final int index;
    private String hsqlIteratorName = null;
    private String rasqlIteratorName = null;

    public ExpressionValueVariableMDA(final int index) {
        this(index, null);
    }

    public ExpressionValueVariableMDA(final int index, String name) {
        super(OpTypes.ARRAY_VALUE_VARIABLE);
        this.index = index;
        this.hsqlIteratorName = name;
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        dataType = Type.SQL_INTEGER;
    }

    @Override
    public Object getValue(final Session session, final boolean isMDARootNode) {

        if (isMDARootNode) {
            throw new IllegalArgumentException("This Expression cannot be a rasRoot");
        }
        
        if (rasqlIteratorName == null) {
            rasqlIteratorName = "x";
        }

        switch (opType) {
            case OpTypes.ARRAY_VALUE_VARIABLE:
                return String.format(rasqlIteratorName + "[%d]", index);

            default :
                throw org.hsqldb.error.Error.runtimeError(ErrorCode.U_S0500, "ExpressionRasValueVariable (type = "+opType+")");
        }
    }

    public void setRasqlIteratorName(String rasqlIteratorName) {
        this.rasqlIteratorName = rasqlIteratorName;
    }

    public String getHsqlIteratorName() {
        return hsqlIteratorName;
    }
    
}
