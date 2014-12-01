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
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.types.Type;

/**
 * Represent infinite dimension bound.
 * 
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class ExpressionIndexUnboundedMDA extends Expression implements ExpressionMDA {

    /**
     * Subset of the form: *:*
     */
    public ExpressionIndexUnboundedMDA() {
        super(OpTypes.ARRAY_SUBSET_ASTERISK);
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {
        dataType = Type.SQL_VARCHAR;
    }

    /**
     * @TODO: implement proper translation to rasql of named subsets
     */
    @Override
    public Object getValue(Session session, boolean isMDARootNode) {
        return "*";
    }
    
}
