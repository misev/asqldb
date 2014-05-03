package org.hsqldb;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.types.Type;

/**
 * Created by Johannes on 5/2/14.
 *
 * @author Johannes Bachhuber
 */
public class ExpressionRasValueVariable extends Expression implements ExpressionRas {

    private final int index;

    public ExpressionRasValueVariable(final int index) {
        super(OpTypes.ARRAY_VALUE_VARIABLE);
        this.index = index;
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        dataType = Type.SQL_INTEGER;
    }

    @Override
    public Object getValue(final Session session, final boolean isRasRoot) {

        if (isRasRoot) {
            throw new IllegalArgumentException("This Expression cannot be a rasRoot");
        }

        switch (opType) {
            case OpTypes.ARRAY_VALUE_VARIABLE:
                return String.format("x[%d]", index);

            default :
                throw org.hsqldb.error.Error.runtimeError(ErrorCode.U_S0500, "ExpressionRasValueVariable (type = "+opType+")");
        }
    }
}
