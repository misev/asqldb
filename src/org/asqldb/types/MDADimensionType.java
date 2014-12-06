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

package org.asqldb.types;

import org.hsqldb.types.RowType;
import org.hsqldb.types.Type;

/**
 * Class for array dimension representation. Each dimension has a name,
 * upper and lower bounds. The lower and/or upper bounds can be unbounded.<p>
 *
 * Grammar:
<pre>
  <array dimension definition> ::=
    <array dimension name> [ <left paren> <array dimension interval> <right paren> ]
  | <array dimension interval>

  <array dimension interval> ::= <array dimension bound> <colon> <array dimension bound>

  <array dimension bound> ::= <numeric value expression> | <array dimension unbounded>
  <array dimension unbounded> ::= *
  <array dimension name> ::= <identifier>
</pre>
 *
 * @author Dimitar Misev
 */
public class MDADimensionType extends RowType {
    
    public static final String DEFAULT_DIMENSION_PREFIX = "d";
    public static final String UNBOUNDED = "*";

    private final String name;
    private final String lowerBound;
    private final String upperBound;
    private final boolean slice;
    
    public MDADimensionType() {
        this(0);
    }
    
    public MDADimensionType(int dimensionIndex) {
        this(null);
    }

    public MDADimensionType(String name) {
        this(name, UNBOUNDED, UNBOUNDED);
    }

    public MDADimensionType(String name, String lowerBound, String upperBound) {
        super(new Type[]{Type.SQL_VARCHAR, Type.SQL_INTEGER, Type.SQL_INTEGER});
        this.name = name;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.slice = false;
    }

    public MDADimensionType(String name, String lowerBound) {
        super(new Type[]{Type.SQL_VARCHAR, Type.SQL_INTEGER});
        this.name = name;
        this.lowerBound = lowerBound;
        this.upperBound = null;
        this.slice = true;
    }
    
    public static String getDefaultName(int dimensionIndex) {
        return DEFAULT_DIMENSION_PREFIX + dimensionIndex;
    }

    public String getDimensionName() {
        return name;
    }

    public String getLowerBound() {
        return lowerBound;
    }

    public String getSlice() {
        return lowerBound;
    }

    public String getUpperBound() {
        return upperBound;
    }
    
    public long getExtent() {
        try {
            return Long.parseLong(upperBound) - Long.parseLong(lowerBound) + 1;
        } catch (Exception ex) {
            return 0;
        }
    }
    
    public boolean isLowerUnbounded() {
        return lowerBound.equals(UNBOUNDED);
    }
    
    public boolean isUpperUnbounded() {
        return upperBound.equals(UNBOUNDED);
    }

    public boolean isSlice() {
        return slice;
    }

    @Override
    public String toString() {
        String ret = lowerBound;
        if (name != null) {
            ret = name + "(" + ret;
        }
        if (!slice) {
            ret += ":" + upperBound;
        }
        if (name != null) {
            ret += ")";
        }
        return ret;
    }
}
