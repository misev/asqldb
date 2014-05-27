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

    private static final int FUNC_RAS_SDOM              = 205;
    private static final int FUNC_RAS_ADD_CELLS         = 206;
    private static final int FUNC_RAS_ALL_CELLS         = 207;
    private static final int FUNC_RAS_AVG_CELLS         = 208;
    private static final int FUNC_RAS_COUNT_CELLS       = 209;
    private static final int FUNC_RAS_MAX_CELLS         = 210;
    private static final int FUNC_RAS_MIN_CELLS         = 211;
    private static final int FUNC_RAS_SOME_CELLS        = 212;
    private static final int FUNC_RAS_ARCCOS            = 213;
    private static final int FUNC_RAS_ARCSIN            = 214;
    private static final int FUNC_RAS_ARCTAN            = 215;
    private static final int FUNC_RAS_BIT               = 216;
    private static final int FUNC_RAS_COMPLEX           = 217;
    private static final int FUNC_RAS_COSH              = 218;
    private static final int FUNC_RAS_DIVIDE            = 219;
    private static final int FUNC_RAS_MODULO            = 220;
    private static final int FUNC_RAS_POW               = 221;
    private static final int FUNC_RAS_SINH              = 222;
    private static final int FUNC_RAS_TANH              = 223;
    private static final int FUNC_RAS_SHIFT             = 224;
    private static final int FUNC_RAS_EXTEND            = 225;


    static final IntKeyIntValueHashMap rasFuncMap =
            new IntKeyIntValueHashMap();

    static {
        rasFuncMap.put(Tokens.RAS_TIFF, FUNC_RAS_TIFF);
        rasFuncMap.put(Tokens.RAS_PNG, FUNC_RAS_PNG);
        rasFuncMap.put(Tokens.RAS_CSV, FUNC_RAS_CSV);
        rasFuncMap.put(Tokens.RAS_JPEG, FUNC_RAS_JPEG);
        rasFuncMap.put(Tokens.RAS_BMP, FUNC_RAS_BMP);

        rasFuncMap.put(Tokens.RAS_SDOM, FUNC_RAS_SDOM);
        rasFuncMap.put(Tokens.RAS_ADD_CELLS, FUNC_RAS_ADD_CELLS);
        rasFuncMap.put(Tokens.RAS_ALL_CELLS, FUNC_RAS_ALL_CELLS);
        rasFuncMap.put(Tokens.RAS_AVG_CELLS, FUNC_RAS_AVG_CELLS);
        rasFuncMap.put(Tokens.RAS_COUNT_CELLS, FUNC_RAS_COUNT_CELLS);
        rasFuncMap.put(Tokens.RAS_MAX_CELLS, FUNC_RAS_MAX_CELLS);
        rasFuncMap.put(Tokens.RAS_MIN_CELLS, FUNC_RAS_MIN_CELLS);
        rasFuncMap.put(Tokens.RAS_SOME_CELLS, FUNC_RAS_SOME_CELLS);
        rasFuncMap.put(Tokens.RAS_ARCCOS, FUNC_RAS_ARCCOS);
        rasFuncMap.put(Tokens.RAS_ARCSIN, FUNC_RAS_ARCSIN);
        rasFuncMap.put(Tokens.RAS_ARCTAN, FUNC_RAS_ARCTAN);
        rasFuncMap.put(Tokens.RAS_BIT, FUNC_RAS_BIT);
        rasFuncMap.put(Tokens.RAS_COMPLEX, FUNC_RAS_COMPLEX);
        rasFuncMap.put(Tokens.RAS_COSH, FUNC_RAS_COSH);
        rasFuncMap.put(Tokens.RAS_DIVIDE, FUNC_RAS_DIVIDE);
        rasFuncMap.put(Tokens.RAS_MODULO, FUNC_RAS_MODULO);
        rasFuncMap.put(Tokens.RAS_POW, FUNC_RAS_POW);
        rasFuncMap.put(Tokens.RAS_SINH, FUNC_RAS_SINH);
        rasFuncMap.put(Tokens.RAS_TANH, FUNC_RAS_TANH);
        rasFuncMap.put(Tokens.RAS_SHIFT, FUNC_RAS_SHIFT);
        rasFuncMap.put(Tokens.RAS_EXTEND, FUNC_RAS_EXTEND);
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
            case FUNC_RAS_SDOM:
            case FUNC_RAS_ADD_CELLS:
            case FUNC_RAS_ALL_CELLS:
            case FUNC_RAS_AVG_CELLS:
            case FUNC_RAS_COUNT_CELLS:
            case FUNC_RAS_MAX_CELLS:
            case FUNC_RAS_MIN_CELLS:
            case FUNC_RAS_SOME_CELLS:
            case FUNC_RAS_ARCCOS:
            case FUNC_RAS_ARCSIN:
            case FUNC_RAS_ARCTAN:
            case FUNC_RAS_COSH:
            case FUNC_RAS_SINH:
            case FUNC_RAS_TANH:
                parseList = singleParamList;
                break;
            case FUNC_RAS_BIT:
            case FUNC_RAS_COMPLEX:
            case FUNC_RAS_DIVIDE:
            case FUNC_RAS_MODULO:
            case FUNC_RAS_POW:
            case FUNC_RAS_SHIFT:
            case FUNC_RAS_EXTEND:
                parseList = doubleParamList;
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

        switch (funcType) {
            case FUNC_RAS_SDOM:
            case FUNC_RAS_SHIFT:
            case FUNC_RAS_EXTEND:
                dataType = Type.SQL_VARCHAR;
                break;
            case FUNC_RAS_ADD_CELLS:
            case FUNC_RAS_AVG_CELLS:
            case FUNC_RAS_COUNT_CELLS:
            case FUNC_RAS_MAX_CELLS:
            case FUNC_RAS_MIN_CELLS:
                dataType = Type.SQL_INTEGER;
                break;
            case FUNC_RAS_ALL_CELLS:
            case FUNC_RAS_SOME_CELLS:
                dataType = Type.SQL_BOOLEAN;
                break;
            case FUNC_RAS_ARCCOS:
            case FUNC_RAS_ARCSIN:
            case FUNC_RAS_ARCTAN:
            case FUNC_RAS_COSH:
            case FUNC_RAS_SINH:
            case FUNC_RAS_TANH:
            case FUNC_RAS_DIVIDE:
            case FUNC_RAS_MODULO:
            case FUNC_RAS_POW:
                dataType = Type.SQL_DECIMAL;
                break;
        }
    }

    /**bb
     * Evaluates and returns this Function in the context of the session.<p>
     */
    @Override
    public Object getValue(Session session) {
        return getValue(session, nodes);
    }

    @Override
    public Object getValue(Session session, boolean isRasRoot) {
        return getValue(session, nodes, isRasRoot);
    }

    /**
     * Evaluates a rasql function.
     * @param session the session
     * @param data parameter data
     * @param isRasRoot
     * @return resulting scalar or link to file
     */
    @Override
    Object getValue(Session session, Object[] data, boolean isRasRoot) {

        switch(funcType) {
            case FUNC_RAS_TIFF:
            case FUNC_RAS_PNG:
            case FUNC_RAS_CSV:
            case FUNC_RAS_JPEG:
            case FUNC_RAS_BMP:
                return getConversionFunctionValue(session);
            case FUNC_RAS_ADD_CELLS:
            case FUNC_RAS_ALL_CELLS:
            case FUNC_RAS_AVG_CELLS:
            case FUNC_RAS_COUNT_CELLS:
            case FUNC_RAS_MAX_CELLS:
            case FUNC_RAS_MIN_CELLS:
            case FUNC_RAS_SOME_CELLS:
            case FUNC_RAS_ARCCOS:
            case FUNC_RAS_ARCSIN:
            case FUNC_RAS_ARCTAN:
            case FUNC_RAS_COSH:
            case FUNC_RAS_SINH:
            case FUNC_RAS_TANH:
                return getSingleParamFunctionValue(session, isRasRoot);
            case FUNC_RAS_SDOM:
                final String functionCall = "sdom(" + nodes[0].getValue(session, false) + ")";
                if (isRasRoot) {
                    return RasUtil.executeHsqlArrayQuery(functionCall, nodes[0].extractRasArrayIds(session));
                }
                return functionCall;

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

    private Object getSingleParamFunctionValue(final Session session, boolean isRasRoot) {
        final Object argValue = nodes[0].getValue(session, false);
        boolean isInt = true;
        String function = null;
        switch(funcType) {
            case FUNC_RAS_ADD_CELLS:
                function = Tokens.T_RAS_ADD_CELLS;
                break;
            case FUNC_RAS_ALL_CELLS:
                function = Tokens.T_RAS_ALL_CELLS;
                break;
            case FUNC_RAS_AVG_CELLS:
                function = Tokens.T_RAS_AVG_CELLS;
                isInt = false;
                break;
            case FUNC_RAS_COUNT_CELLS:
                function = Tokens.T_RAS_COUNT_CELLS;
                break;
            case FUNC_RAS_MAX_CELLS:
                function = Tokens.T_RAS_MAX_CELLS;
                break;
            case FUNC_RAS_MIN_CELLS:
                function = Tokens.T_RAS_MIN_CELLS;
                break;
            case FUNC_RAS_SOME_CELLS:
                function = Tokens.T_RAS_SOME_CELLS;
                break;
            case FUNC_RAS_ARCCOS:
                function = Tokens.T_RAS_ARCCOS;
                isInt = false;
                break;
            case FUNC_RAS_ARCSIN:
                function = Tokens.T_RAS_ARCSIN;
                isInt = false;
                break;
            case FUNC_RAS_ARCTAN:
                function = Tokens.T_RAS_ARCTAN;
                isInt = false;
                break;
            case FUNC_RAS_COSH:
                function = Tokens.T_RAS_COSH;
                isInt = false;
                break;
            case FUNC_RAS_SINH:
                function = Tokens.T_RAS_SINH;
                isInt = false;
                break;
            case FUNC_RAS_TANH:
                function = Tokens.T_RAS_TANH;
                isInt = false;
                break;

        }
        if (function != null) {
            final String functionCall = String.format("%s(%s)", function, argValue);
            if (!isRasRoot) {
                return functionCall;
            }
            final String ret = RasUtil.executeHsqlArrayQuery(functionCall,
                    nodes[0].extractRasArrayIds(session));
            if (isInt)
                return Integer.valueOf(ret);
            else
                return Double.valueOf(ret);
        }
        throw Error.runtimeError(ErrorCode.U_S0500, "Required: aggregate function");
    }
}
