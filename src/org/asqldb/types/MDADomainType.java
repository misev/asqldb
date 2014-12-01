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
import org.hsqldb.error.ErrorCode;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

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
public class MDADomainType extends ArrayType {
    
    public static final int MAX_DIMENSIONALITY = 1000;

    private final List<MDADimensionType> dimensions;
    private int cardinality = 0;

    public MDADomainType() {
        super(Type.SQL_MDADIMENSION_TYPE, MAX_DIMENSIONALITY);
        dimensions = new ArrayList<MDADimensionType>();
    }
    
    public void addDimension(MDADimensionType dimension) {
        dimensions.add(dimension);
        cardinality *= dimension.getExtent();
    }
    
    public MDADimensionType getDimension(int index) {
        if (index >= getDimensionality()) {
            throw new IllegalArgumentException("Tried to access dimension " +
                    index + " in a " + getDimensionality() + "-D array.");
        }
        return dimensions.get(index);
    }
    
    public MDADimensionType getDimension(String name) {
        for (MDADimensionType dimension : dimensions) {
            if (dimension.getDimensionName().equals(name)) {
                return dimension;
            }
        }
        throw new IllegalArgumentException("Dimension with name " + name + " not found.");
    }
    
    /**
     * @return the index of dimension with given name, or -1 if not found.
     */
    public int getDimensionIndex(String name) {
        int i = 0;
        for (MDADimensionType dimension : dimensions) {
            if (dimension.getDimensionName().equals(name)) {
                return i;
            }
            ++i;
        }
        return -1;
    }
    
    public boolean hasDimension(String name) {
        boolean ret = false;
        if (name != null) {
            for (MDADimensionType dimension : dimensions) {
                if (dimension.getDimensionName().equals(name)) {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }
    
    public int getDimensionality() {
        return dimensions.size();
    }

    public int getCardinality() {
        return cardinality;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("");
        for (MDADimensionType dimension : dimensions) {
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
    
    public boolean isNamedSubset(MDADomainType parentSubset) {
        boolean ret = true;
        for (MDADimensionType dimension : dimensions) {
            if (!parentSubset.hasDimension(dimension.getDimensionName())) {
                ret = false;
                break;
            }
        }
        return ret;
    }
    
    /**
     * Remove slices and set dimension names properly in the subset domain.
     */
    public MDADomainType matchSubsetDomain(MDADomainType subsetDomain) {
        if (subsetDomain.isNamedSubset(this)) {
            return matchNamedSubsetDomain(subsetDomain);
        } else {
            return matchPositionSubsetDomain(subsetDomain);
        }
    }
    
    /**
     * Normalize subset domain that has named subsets, e.g. x(5)
     */
    private MDADomainType matchNamedSubsetDomain(MDADomainType subsetDomain) {
        MDADomainType ret = new MDADomainType();
        for (int i = 0; i < getDimensionality(); i++) {
            MDADimensionType dimension = getDimension(i);
            MDADimensionType newDimension = null;
            try {
                newDimension = subsetDomain.getDimension(dimension.getDimensionName());
            } catch (IllegalArgumentException ex) {
                newDimension = dimension;
            }
            if (!newDimension.isSlice()) {
                ret.addDimension(newDimension);
            }
        }
        return ret;
    }
    
    /**
     * Normalize subset domain that has position subsets, e.g. 0:14,1,..
     */
    private MDADomainType matchPositionSubsetDomain(MDADomainType subsetDomain) {
        MDADomainType ret = new MDADomainType();
        if (getDimensionality() != subsetDomain.getDimensionality()) {
            throw org.hsqldb.error.Error.error(ErrorCode.MDA_INVALID_SUBSET,
                    "Subset dimensionality does not match array dimensionality.");
        }
        for (int i = 0; i < subsetDomain.getDimensionality(); i++) {
            MDADimensionType newDimension = subsetDomain.getDimension(i);
            if (!newDimension.isSlice()) {
                ret.addDimension(newDimension);
            }
        }
        return ret;
    }
}
