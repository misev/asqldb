package org.hsqldb;

import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.ras.RasArrayId;
import org.hsqldb.ras.RasUtil;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

import java.util.Set;

/**
 * Created by Johannes on 5/9/14.
 *
 * @author Johannes Bachhuber
 */
public class ExpressionRasAggregate extends Expression implements ExpressionRas {

    public ExpressionRasAggregate(final int type, final Expression left, final Expression right) {
        super(type);
        nodes = new Expression[BINARY];
        nodes[LEFT] = left;
        nodes[RIGHT] = right;
    }

    @Override
    public void resolveTypes(final Session session, final Expression parent) {
        for (Expression node : nodes) {
            if (node != null) {
                node.resolveTypes(session, this);
            }
        }
        dataType = Type.SQL_VARCHAR;
    }

    @Override
    public Object getValue(final Session session, final boolean isRasRoot) {
        final String condense = String.format("CONDENSE %s OVER x in %s USING %s",
                Tokens.getKeyword(opType), nodes[LEFT].getValue(session, false),
                nodes[RIGHT].getValue(session, false));

        if (isRasRoot) {
            final Set<RasArrayId> rasArrayIds = nodes[LEFT].getRasArrayIds(session);
            rasArrayIds.addAll(nodes[RIGHT].getRasArrayIds(session));
            return RasUtil.executeHsqlArrayQuery(condense, rasArrayIds);
        }
        return condense;
    }
}
