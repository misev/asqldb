/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.asqldb.util;

import java.util.Iterator;
import org.hsqldb.error.ErrorCode;
import rasj.RasDimensionMismatchException;
import rasj.RasIndexOutOfBoundsException;
import rasj.RasMInterval;
import rasj.RasPoint;

/**
 *
 * @author dimitar
 */
public class RowMajorIterator implements Iterator<RasPoint> {

    private final RasPoint origin;
    private final RasPoint high;
    private final int dimension;
    private RasPoint iter;

    public RowMajorIterator(RasPoint origin, RasPoint end) {
        this.origin = origin;
        this.high = end;
        this.iter = null;
        this.dimension = origin.dimension();
        if (dimension != end.dimension()) {
            throw org.hsqldb.error.Error.runtimeError(
                    ErrorCode.U_S0500, "Mismatched bounding box.");
        }
    }

    public RowMajorIterator(RasMInterval sdom) {
        this(sdom.getOrigin(), sdom.getHigh());
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
                iter = new RasPoint(origin);
            } else {
                for (int i = dimension - 1; i >= 0; i--) {
                    long currVal = iter.item(i);
                    if (currVal >= high.item(i)) {
                        iter.setItem(i, origin.item(i));
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
        iter = origin;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
