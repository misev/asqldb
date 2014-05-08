package org.hsqldb;

import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.ras.RasArrayId;
import org.hsqldb.ras.RasUtil;

import java.util.Set;

/**
 * Created by Johannes on 4/16/14.
 * @author Johannes Bachhuber
 */
public class ExpressionAccessorRas extends ExpressionAccessor implements ExpressionRas {

    ExpressionAccessorRas(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public Object getValue(Session session, boolean isRasRoot) {
        final String index = (nodes[RIGHT]==null)?"":("[" + nodes[RIGHT].getValue(session, false) + "]");
        final String colName = nodes[LEFT].getColumnName();

        if (isRasRoot) {
            Set<RasArrayId> rasArrayIds = nodes[LEFT].extractRasArrayIds(session);
            rasArrayIds.addAll(nodes[RIGHT].extractRasArrayIds(session));
            return RasUtil.executeHsqlArrayQuery(colName + index, rasArrayIds);
        }
        return colName+index;
    }
}
