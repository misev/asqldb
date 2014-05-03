package org.hsqldb;

import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

/**
 * Created by Johannes on 5/1/14.
 *
 * @author Johannes Bachhuber
 */
public class ExpressionRasDimensionLiteral extends Expression implements ExpressionRas {

    private HsqlNameManager.SimpleName name;
    private final int index;

    public ExpressionRasDimensionLiteral(final HsqlNameManager.SimpleName name, final int index, final Expression range) {
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
