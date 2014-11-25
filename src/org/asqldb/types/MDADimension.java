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
public class MDADimension {
    
    public static final String DEFAULT_DIMENSION_PREFIX = "d";
    public static final Long LOWER_UNBOUNDED = Long.MIN_VALUE;
    public static final Long UPPER_UNBOUNDED = Long.MAX_VALUE;

    private final String name;
    private final long lowerBound;
    private final long upperBound;
    
    public MDADimension() {
        this(0);
    }
    
    public MDADimension(int dimensionIndex) {
        this(MDADimension.getDefaultName(dimensionIndex));
    }

    public MDADimension(String name) {
        this(name, LOWER_UNBOUNDED, UPPER_UNBOUNDED);
    }

    public MDADimension(String name, long lowerBound, long upperBound) {
        this.name = name;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public MDADimension(int dimensionIndex, long lowerBound, long upperBound) {
        this(MDADimension.getDefaultName(dimensionIndex), lowerBound, upperBound);
    }

    public String getName() {
        return name;
    }
    
    public static String getDefaultName(int dimensionIndex) {
        return DEFAULT_DIMENSION_PREFIX + dimensionIndex;
    }

    public long getLowerBound() {
        return lowerBound;
    }

    public long getUpperBound() {
        return upperBound;
    }
    
    public long getExtent() {
        return upperBound - lowerBound + 1;
    }
    
    public boolean isLowerUnbounded() {
        return lowerBound == LOWER_UNBOUNDED;
    }
    
    public boolean isUpperUnbounded() {
        return upperBound == UPPER_UNBOUNDED;
    }

    @Override
    public String toString() {
        return name + "(" + lowerBound + ":" + upperBound + ")";
    }
}
