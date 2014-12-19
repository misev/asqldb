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
package org.asqldb.util;

import java.util.Iterator;
import org.hsqldb.error.ErrorCode;
import rasj.RasDimensionMismatchException;
import rasj.RasIndexOutOfBoundsException;
import rasj.RasMInterval;
import rasj.RasPoint;

/**
 * Iterate a point over a multidimensional interval.
 *
 * @author Dimitar Misev
 */
public class RasMIntervalIterator implements Iterator<RasPoint> {

    private final RasPoint low;
    private final RasPoint high;
    private final int dimension;
    private RasPoint iter;

    public RasMIntervalIterator(RasPoint low, RasPoint high) {
        this.low = low;
        this.high = high;
        this.iter = null;
        this.dimension = low.dimension();
        if (dimension != high.dimension()) {
            throw org.hsqldb.error.Error.runtimeError(
                    ErrorCode.U_S0500, "Mismatched bounding box.");
        }
    }

    public RasMIntervalIterator(RasMInterval domain) {
        this(domain.getOrigin(), domain.getHigh());
    }

    @Override
    public boolean hasNext() {
        try {
            return iter == null || !iter.equals(high);
        } catch (RasDimensionMismatchException ex) {
            return false;
        }
    }

    @Override
    public RasPoint next() {
        try {
            if (iter == null) {
                iter = new RasPoint(low);
            } else {
                for (int i = dimension - 1; i >= 0; i--) {
                    long currVal = iter.item(i);
                    if (currVal >= high.item(i)) {
                        iter.setItem(i, low.item(i));
                    } else {
                        iter.setItem(i, currVal + 1);
                        break;
                    }
                }
            }
        } catch (RasIndexOutOfBoundsException ex) {
            throw org.hsqldb.error.Error.runtimeError(
                    ErrorCode.U_S0500, ex.getMessage());
        }
        return iter;
    }

    public void reset() {
        iter = low;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
