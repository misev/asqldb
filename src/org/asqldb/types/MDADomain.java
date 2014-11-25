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

import java.util.ArrayList;
import java.util.List;

/**
 * Class for MDARRAY domain representation. The domain is simply a list
 * of dimension definitions.<p>
 *
 * Grammar:
<pre>
  <array type> ::= 
    <data type> MDARRAY [ <array definition domain> ]
    
  <array definition domain> ::=
    <left bracket or trigraph> <array dimension definition list> <left bracket or trigraph>

  <array dimension definition list> ::=
    <array dimension definition> [ { <comma> <array dimension definition> } ... ]
</pre>
 *  
 * @author Dimitar Misev
 */
public class MDADomain {

    private final List<MDADimension> dimensions;
    private long cardinality;

    public MDADomain() {
        this.dimensions = new ArrayList<>();
        cardinality = 0;
    }
    
    public void addDimension(MDADimension dimension) {
        this.dimensions.add(dimension);
        cardinality += dimension.getExtent();
    }
    
    public MDADimension getDimension(int index) {
        if (index >= getDimensionality()) {
            throw new IllegalArgumentException("Tried to access dimension " +
                    index + " in a " + getDimensionality() + "-D array.");
        }
        return dimensions.get(index);
    }
    
    public MDADimension getDimension(String name) {
        for (MDADimension dimension : dimensions) {
            if (dimension.getName().equals(name)) {
                return dimension;
            }
        }
        throw new IllegalArgumentException("Dimension with name " + name + " not found.");
    }
    
    public int getDimensionality() {
        return dimensions.size();
    }
    
    public long getCardinality() {
        return cardinality;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("");
        for (MDADimension dimension : dimensions) {
            if (res.length() > 0) {
                res.append(",");
            }
            res.append(dimension.toString());
        }
        
        String ret = res.toString();
        if (ret.length() > 0) {
            ret = "[" + ret + "]";
        }
        return ret;
    }
    
    public String toRasqlString() {
        String ret = "";
        
        return ret;
    }
}
