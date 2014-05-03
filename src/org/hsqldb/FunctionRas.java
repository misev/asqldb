package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.hsqldb.ras.ExpressionRas;
import org.hsqldb.ras.RasUtil;
import org.hsqldb.types.Type;

/**
 * Created by Johannes on 4/9/14.
 * @author Johannes Bachhuber
 */
public class FunctionRas extends FunctionSQL implements ExpressionRas {

    private static FrameworkLogger log = FrameworkLogger.getLog(FunctionRas.class);

    private static final int FUNC_RAS_TIFF              = 200;
    private static final int FUNC_RAS_PNG               = 201;
    private static final int FUNC_RAS_CSV               = 202;
    private static final int FUNC_RAS_JPEG              = 203;
    private static final int FUNC_RAS_BMP               = 204;

    //function to get the string oid value of an array column
    private static final int FUNC_RAS_COLVAL            = 205;



    static final IntKeyIntValueHashMap rasFuncMap =
            new IntKeyIntValueHashMap();

    static {
        rasFuncMap.put(Tokens.RAS_TIFF, FUNC_RAS_TIFF);
        rasFuncMap.put(Tokens.RAS_PNG, FUNC_RAS_PNG);
        rasFuncMap.put(Tokens.RAS_CSV, FUNC_RAS_CSV);
        rasFuncMap.put(Tokens.RAS_JPEG, FUNC_RAS_JPEG);
        rasFuncMap.put(Tokens.RAS_BMP, FUNC_RAS_BMP);

        rasFuncMap.put(Tokens.RAS_COLVAL, FUNC_RAS_COLVAL);
    }

    protected FunctionRas(int id) {
        super();
        this.funcType = id;

        switch(id) {
            case FUNC_RAS_TIFF:
            case FUNC_RAS_PNG:
            case FUNC_RAS_CSV:
            case FUNC_RAS_JPEG:
            case FUNC_RAS_BMP:
            case FUNC_RAS_COLVAL:
                parseList = singleParamList;
                break;
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionRas");
        }
    }

    public static FunctionRas newRasFunction(int tokenType) {
        int id = rasFuncMap.get(tokenType, -1);
        if (id == -1)
            return null;
        return new FunctionRas(id);
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        for (Expression node : nodes) {
            if (node != null) {
                node.resolveTypes(session, this);
            }
        }

        switch (opType) {
            case FUNC_RAS_COLVAL:
                this.dataType = Type.SQL_VARCHAR;
        }
    }

    /**bb
     * Evaluates and returns this Function in the context of the session.<p>
     */
    @Override
    public Object getValue(Session session) {
        return getValue(session, nodes);
    }

    /**
     * Evaluates a rasql function.
     * @param session the session
     * @param data parameter data
     * @return resulting scalar or link to file
     */
    @Override
    Object getValue(Session session, Object[] data) {

        switch(funcType) {
            case FUNC_RAS_TIFF:
            case FUNC_RAS_PNG:
            case FUNC_RAS_CSV:
            case FUNC_RAS_JPEG:
            case FUNC_RAS_BMP:
                return getConversionFunctionValue(session);

            case FUNC_RAS_COLVAL:
                if (!(nodes[0] instanceof ExpressionColumn))
                    throw Error.error(ErrorCode.RAS_INVALID_PARAMETER, nodes[0].getClass().getSimpleName()+" expected: ExpressionColumn");
                return RasUtil.objectArrayToString(((ExpressionColumn) nodes[0]).getHsqlColumnValue(session));
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionRas");
        }
    }

    private Object getConversionFunctionValue(Session session) {
        final Object argValue = nodes[0].getValue(session, false);
        final String argString = (argValue instanceof Object[])?
                RasUtil.objectArrayToString(argValue):
                (String)argValue;

        switch(funcType) {
            case FUNC_RAS_TIFF:
                log.info("Executing function tiff: nodes[0] = "+ nodes[0]);
                return RasUtil.executeHsqlArrayQuery("tiff("+ argString +")", ".tiff", nodes[0].extractRasArrayIds(session));
            case FUNC_RAS_PNG:
                log.info("Executing function png: nodes[0] = "+ nodes[0]);
                return RasUtil.executeHsqlArrayQuery("png("+ argString +")", ".png", nodes[0].extractRasArrayIds(session));
            case FUNC_RAS_CSV:
                log.info("Executing function csv: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("csv("+ argString +")", ".csv", nodes[0].extractRasArrayIds(session));
            case FUNC_RAS_JPEG:
                log.info("Executing function jpeg: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("jpeg("+ argString +")", ".jpeg",nodes[0].extractRasArrayIds(session));
            case FUNC_RAS_BMP:
                log.info("Executing function bmp: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("bmp("+ argString +")", ".bmp", nodes[0].extractRasArrayIds(session));
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionRas");

        }
    }
}
