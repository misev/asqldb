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
package org.asqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.asqldb.ras.RasUtil;
import org.hsqldb.types.Type;

import org.asqldb.ras.RasArrayIdSet;
import org.asqldb.types.MDADimensionType;
import org.asqldb.types.MDADomainType;
import org.asqldb.types.MDAType;
import org.hsqldb.Expression;
import org.hsqldb.FunctionSQL;
import org.hsqldb.Session;
import org.hsqldb.Tokens;
import org.hsqldb.types.BlobDataID;
import rasj.RasGMArray;

/**
 * @author Johannes Bachhuber
 * @author Dimitar Misev
 */
public class FunctionMDA extends FunctionSQL implements ExpressionMDA {

    private static FrameworkLogger log = FrameworkLogger.getLog(FunctionMDA.class);

    private static final int FUNC_MDA_TIFF = 200;
    private static final int FUNC_MDA_PNG = 201;
    private static final int FUNC_MDA_CSV = 202;
    private static final int FUNC_MDA_JPEG = 203;
    private static final int FUNC_MDA_BMP = 204;

    private static final int FUNC_MDA_SDOM = 205;
    private static final int FUNC_MDA_ADD_CELLS = 206;
    private static final int FUNC_MDA_ALL_CELLS = 207;
    private static final int FUNC_MDA_AVG_CELLS = 208;
    private static final int FUNC_MDA_COUNT_CELLS = 209;
    private static final int FUNC_MDA_MAX_CELLS = 210;
    private static final int FUNC_MDA_MIN_CELLS = 211;
    private static final int FUNC_MDA_SOME_CELLS = 212;
    private static final int FUNC_MDA_ARCCOS = 213;
    private static final int FUNC_MDA_ARCSIN = 214;
    private static final int FUNC_MDA_ARCTAN = 215;
    private static final int FUNC_MDA_BIT = 216;
    private static final int FUNC_MDA_COMPLEX = 217;
    private static final int FUNC_MDA_COSH = 218;
    private static final int FUNC_MDA_DIVIDE = 219;
    private static final int FUNC_MDA_MODULO = 220;
    private static final int FUNC_MDA_POW = 221;
    private static final int FUNC_MDA_SINH = 222;
    private static final int FUNC_MDA_TANH = 223;
    private static final int FUNC_MDA_SHIFT = 224;
    private static final int FUNC_MDA_EXTEND = 225;
    private static final int FUNC_MDA_DIV = 226;
    private static final int FUNC_MDA_SCALE = 227;

    private static final int FUNC_MDA_DECODE = 250;
    private static final int FUNC_MDA_ENCODE = 251;
    
    private static final int FUNC_MDA_LO = 260;
    private static final int FUNC_MDA_HI = 261;
    private static final int FUNC_MDA_NAME = 262;
    private static final int FUNC_MDA_DIMENSION = 263;

    static final IntKeyIntValueHashMap mdaFuncMap
            = new IntKeyIntValueHashMap();

    static {
        mdaFuncMap.put(Tokens.MDA_TIFF, FUNC_MDA_TIFF);
        mdaFuncMap.put(Tokens.MDA_PNG, FUNC_MDA_PNG);
        mdaFuncMap.put(Tokens.MDA_CSV, FUNC_MDA_CSV);
        mdaFuncMap.put(Tokens.MDA_JPEG, FUNC_MDA_JPEG);
        mdaFuncMap.put(Tokens.MDA_BMP, FUNC_MDA_BMP);

        mdaFuncMap.put(Tokens.MDA_SDOM, FUNC_MDA_SDOM);
        mdaFuncMap.put(Tokens.MDA_ADD_CELLS, FUNC_MDA_ADD_CELLS);
        mdaFuncMap.put(Tokens.MDA_ALL_CELLS, FUNC_MDA_ALL_CELLS);
        mdaFuncMap.put(Tokens.MDA_AVG_CELLS, FUNC_MDA_AVG_CELLS);
        mdaFuncMap.put(Tokens.MDA_COUNT_CELLS, FUNC_MDA_COUNT_CELLS);
        mdaFuncMap.put(Tokens.MDA_MAX_CELLS, FUNC_MDA_MAX_CELLS);
        mdaFuncMap.put(Tokens.MDA_MIN_CELLS, FUNC_MDA_MIN_CELLS);
        mdaFuncMap.put(Tokens.MDA_SOME_CELLS, FUNC_MDA_SOME_CELLS);
        mdaFuncMap.put(Tokens.MDA_ARCCOS, FUNC_MDA_ARCCOS);
        mdaFuncMap.put(Tokens.MDA_ARCSIN, FUNC_MDA_ARCSIN);
        mdaFuncMap.put(Tokens.MDA_ARCTAN, FUNC_MDA_ARCTAN);
        mdaFuncMap.put(Tokens.MDA_BIT, FUNC_MDA_BIT);
        mdaFuncMap.put(Tokens.MDA_COMPLEX, FUNC_MDA_COMPLEX);
        mdaFuncMap.put(Tokens.MDA_COSH, FUNC_MDA_COSH);
        mdaFuncMap.put(Tokens.MDA_DIVIDE, FUNC_MDA_DIVIDE);
        mdaFuncMap.put(Tokens.MDA_MODULO, FUNC_MDA_MODULO);
        mdaFuncMap.put(Tokens.MDA_POW, FUNC_MDA_POW);
        mdaFuncMap.put(Tokens.MDA_SINH, FUNC_MDA_SINH);
        mdaFuncMap.put(Tokens.MDA_TANH, FUNC_MDA_TANH);
        mdaFuncMap.put(Tokens.MDA_SHIFT, FUNC_MDA_SHIFT);
        mdaFuncMap.put(Tokens.MDA_EXTEND, FUNC_MDA_EXTEND);
        mdaFuncMap.put(Tokens.MDA_DIV, FUNC_MDA_DIV);
        mdaFuncMap.put(Tokens.MDA_SCALE, FUNC_MDA_SCALE);
        mdaFuncMap.put(Tokens.MDA_DECODE, FUNC_MDA_DECODE);
        mdaFuncMap.put(Tokens.MDA_ENCODE, FUNC_MDA_ENCODE);
        
        mdaFuncMap.put(Tokens.MDA_LO, FUNC_MDA_LO);
        mdaFuncMap.put(Tokens.MDA_HI, FUNC_MDA_HI);
        mdaFuncMap.put(Tokens.MDA_DIMENSION_NAME, FUNC_MDA_NAME);
        mdaFuncMap.put(Tokens.MDA_DIMENSION, FUNC_MDA_DIMENSION);
    }

    protected FunctionMDA(int id) {
        super();
        this.funcType = id;

        switch (id) {
            case FUNC_MDA_TIFF:
            case FUNC_MDA_PNG:
            case FUNC_MDA_CSV:
            case FUNC_MDA_JPEG:
            case FUNC_MDA_BMP:
            case FUNC_MDA_SDOM:
            case FUNC_MDA_ADD_CELLS:
            case FUNC_MDA_ALL_CELLS:
            case FUNC_MDA_AVG_CELLS:
            case FUNC_MDA_COUNT_CELLS:
            case FUNC_MDA_MAX_CELLS:
            case FUNC_MDA_MIN_CELLS:
            case FUNC_MDA_SOME_CELLS:
            case FUNC_MDA_ARCCOS:
            case FUNC_MDA_ARCSIN:
            case FUNC_MDA_ARCTAN:
            case FUNC_MDA_COSH:
            case FUNC_MDA_SINH:
            case FUNC_MDA_TANH:
            case FUNC_MDA_DECODE:
            case FUNC_MDA_DIMENSION:
                parseList = singleParamList;
                break;
            case FUNC_MDA_BIT:
            case FUNC_MDA_COMPLEX:
            case FUNC_MDA_DIVIDE:
            case FUNC_MDA_MODULO:
            case FUNC_MDA_POW:
            case FUNC_MDA_SHIFT:
            case FUNC_MDA_EXTEND:
            case FUNC_MDA_DIV:
            case FUNC_MDA_ENCODE:
            case FUNC_MDA_LO:
            case FUNC_MDA_HI:
            case FUNC_MDA_NAME:
            case FUNC_MDA_SCALE:
                parseList = doubleParamList;
                break;
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionMDA");
        }
    }

    public static FunctionMDA newFunctionMDA(int tokenType) {
        int id = mdaFuncMap.get(tokenType, -1);
        if (id == -1) {
            return null;
        }
        return new FunctionMDA(id);
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {
        resolveChildrenTypes(session);

        switch (funcType) {
            /**
             * @TODO: sdom should be a proper SQL type, not string.
             */
            case FUNC_MDA_SDOM:
            case FUNC_MDA_NAME:
                dataType = Type.SQL_VARCHAR;
                break;
            case FUNC_MDA_ADD_CELLS:
            case FUNC_MDA_AVG_CELLS:
            case FUNC_MDA_MAX_CELLS:
            case FUNC_MDA_MIN_CELLS:
                dataType = Type.SQL_DOUBLE;
            case FUNC_MDA_COUNT_CELLS:
            case FUNC_MDA_DIV:
            case FUNC_MDA_LO:
            case FUNC_MDA_HI:
            case FUNC_MDA_DIMENSION:
                dataType = Type.SQL_INTEGER;
                break;
            case FUNC_MDA_ALL_CELLS:
            case FUNC_MDA_SOME_CELLS:
                dataType = Type.SQL_BOOLEAN;
                break;
            case FUNC_MDA_ARCCOS:
            case FUNC_MDA_ARCSIN:
            case FUNC_MDA_ARCTAN:
            case FUNC_MDA_COSH:
            case FUNC_MDA_SINH:
            case FUNC_MDA_TANH:
            case FUNC_MDA_DIVIDE:
            case FUNC_MDA_MODULO:
            case FUNC_MDA_POW:
                dataType = Type.SQL_DECIMAL;
                break;
            case FUNC_MDA_DECODE:
                nodes[LEFT].setDataType(Type.SQL_BLOB);
                dataType = Type.SQL_MDARRAY_ALL_TYPES;
                break;
            case FUNC_MDA_ENCODE:
                dataType = Type.SQL_ALL_TYPES;
                break;
            case FUNC_MDA_SHIFT:
            case FUNC_MDA_EXTEND:
            case FUNC_MDA_SCALE:
                dataType = nodes[LEFT].getDataType();
                break;
        }
    }

    /**
     * Evaluates and returns this Function in the context of the session.<p>
     */
    @Override
    public Object getValue(Session session) {
        return getValue(session, nodes);
    }

    /**
     * Evaluates a rasql function.
     *
     * @param session the session
     * @param data parameter data
     * @param isMDARootNode
     * @return resulting scalar or link to file
     */
    @Override
    protected Object getValue(Session session, Object[] data, boolean isMDARootNode) {

        switch (funcType) {
            case FUNC_MDA_TIFF:
            case FUNC_MDA_PNG:
            case FUNC_MDA_CSV:
            case FUNC_MDA_JPEG:
            case FUNC_MDA_BMP:
            case FUNC_MDA_ENCODE:
                return getConversionFunctionValue(session);
            case FUNC_MDA_ADD_CELLS:
            case FUNC_MDA_ALL_CELLS:
            case FUNC_MDA_AVG_CELLS:
            case FUNC_MDA_COUNT_CELLS:
            case FUNC_MDA_MAX_CELLS:
            case FUNC_MDA_MIN_CELLS:
            case FUNC_MDA_SOME_CELLS:
            case FUNC_MDA_ARCCOS:
            case FUNC_MDA_ARCSIN:
            case FUNC_MDA_ARCTAN:
            case FUNC_MDA_COSH:
            case FUNC_MDA_SINH:
            case FUNC_MDA_TANH:
            case FUNC_MDA_DECODE:
                return getSingleParamFunctionValue(session, isMDARootNode);
            case FUNC_MDA_BIT:
            case FUNC_MDA_COMPLEX:
            case FUNC_MDA_DIVIDE:
            case FUNC_MDA_MODULO:
            case FUNC_MDA_POW:
            case FUNC_MDA_SHIFT:
            case FUNC_MDA_EXTEND:
            case FUNC_MDA_DIV:
            case FUNC_MDA_SCALE:
                return getDoubleParamFunctionValue(session, isMDARootNode);
            case FUNC_MDA_LO:
            case FUNC_MDA_HI:
                return getLoHiValue(session, isMDARootNode);

            case FUNC_MDA_SDOM:
                final String functionCall = "sdom(" + nodes[0].getValue(session, false) + ")";
                if (isMDARootNode) {
                    return RasUtil.executeHsqlArrayQuery(functionCall, nodes[0].getRasArrayIds(session));
                }
                return functionCall;
                
            case FUNC_MDA_NAME:
                Integer index = (Integer) nodes[RIGHT].getValue(session, false);
                return getNameForIndex(index);
                
            case FUNC_MDA_DIMENSION:
                return getDimensionality();

            default:
                throw Error.runtimeError(ErrorCode.U_S0500, opType + "");
        }
    }

    private Object getConversionFunctionValue(Session session) {
        final Object argValue = nodes[0].getValue(session, false);
        final String argString = (argValue instanceof Object[])
                ? RasUtil.objectArrayToString(argValue) : (String) argValue;

        RasArrayIdSet rasArrayIds = nodes[0].getRasArrayIds(session);

        switch (funcType) {
            case FUNC_MDA_TIFF:
                log.info("Executing function tiff: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("tiff(" + argString + ")", ".tiff", rasArrayIds);
            case FUNC_MDA_PNG:
                log.info("Executing function png: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("png(" + argString + ")", ".png", rasArrayIds);
            case FUNC_MDA_CSV:
                log.info("Executing function csv: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("csv(" + argString + ")", ".csv", rasArrayIds);
            case FUNC_MDA_JPEG:
                log.info("Executing function jpeg: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("jpeg(" + argString + ")", ".jpeg", rasArrayIds);
            case FUNC_MDA_BMP:
                log.info("Executing function bmp: nodes[0] = " + nodes[0]);
                return RasUtil.executeHsqlArrayQuery("bmp(" + argString + ")", ".bmp", rasArrayIds);
            case FUNC_MDA_ENCODE:
                log.info("Executing function encode: nodes[0] = " + nodes[0]);

                final Object formatObj = nodes[1].getValue(session, false);
                final String format = (formatObj instanceof Object[])
                        ? RasUtil.objectArrayToString(formatObj) : (String) formatObj;

                String field = rasArrayIds.stringifyIdentifier();
                final String rasql = "select encode(" + argString + ", \"" + format + "\") from "
                        + rasArrayIds.stringifyRasColls() + " WHERE " + rasArrayIds.stringifyOids();
                return RasUtil.executeRasqlQuery(rasql, false);
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionRas");

        }
    }

    private Object getSingleParamFunctionValue(final Session session, boolean isMDARootNode) {
        final Object argValue = nodes[0].getValue(session, false);
        boolean isInt = true;
        String function = null;
        switch (funcType) {
            case FUNC_MDA_ADD_CELLS:
                function = Tokens.T_MDA_ADD_CELLS;
                break;
            case FUNC_MDA_ALL_CELLS:
                function = Tokens.T_MDA_ALL_CELLS;
                break;
            case FUNC_MDA_AVG_CELLS:
                function = Tokens.T_MDA_AVG_CELLS;
                isInt = false;
                break;
            case FUNC_MDA_COUNT_CELLS:
                function = Tokens.T_MDA_COUNT_CELLS;
                break;
            case FUNC_MDA_MAX_CELLS:
                function = Tokens.T_MDA_MAX_CELLS;
                break;
            case FUNC_MDA_MIN_CELLS:
                function = Tokens.T_MDA_MIN_CELLS;
                break;
            case FUNC_MDA_SOME_CELLS:
                function = Tokens.T_MDA_SOME_CELLS;
                break;
            case FUNC_MDA_ARCCOS:
                function = Tokens.T_MDA_ARCCOS;
                isInt = false;
                break;
            case FUNC_MDA_ARCSIN:
                function = Tokens.T_MDA_ARCSIN;
                isInt = false;
                break;
            case FUNC_MDA_ARCTAN:
                function = Tokens.T_MDA_ARCTAN;
                isInt = false;
                break;
            case FUNC_MDA_COSH:
                function = Tokens.T_MDA_COSH;
                isInt = false;
                break;
            case FUNC_MDA_SINH:
                function = Tokens.T_MDA_SINH;
                isInt = false;
                break;
            case FUNC_MDA_TANH:
                function = Tokens.T_MDA_TANH;
                isInt = false;
                break;
        }
        if (funcType == FUNC_MDA_DECODE) {
            if (insertColumn != null) {
                if (argValue != null && argValue instanceof BlobDataID) {
                    final BlobDataID blob = (BlobDataID) argValue;
                    final byte[] bis = blob.getBytes(session, 0, (int) blob.length(session));
                    final RasGMArray blobArray = RasUtil.convertBlobToArray(bis);
                    final String rasql = "INSERT INTO " + insertColumn.getRasdamanCollectionName() + " VALUES decode($1)";
                    final Object result = RasUtil.executeRasqlQuery(rasql, true, true, blobArray);
                    return RasUtil.dbagToOid(result);
                }
            }
        } else if (function != null) {
            final String functionCall = String.format("%s(%s)", function, argValue);
            if (!isMDARootNode) {
                return functionCall;
            }
            final String ret = RasUtil.executeHsqlArrayQuery(functionCall,
                    nodes[0].getRasArrayIds(session)).toString();
            if (isInt) {
                return Integer.valueOf(ret);
            } else {
                return Double.valueOf(ret);
            }
        }
        throw Error.runtimeError(ErrorCode.U_S0500, "Required: aggregate function");
    }

    private Object getDoubleParamFunctionValue(final Session session, boolean isMDARootNode) {
        String function = null;
        switch (funcType) {
            case FUNC_MDA_BIT:
                function = "bit";
                break;
            case FUNC_MDA_COMPLEX:
                function = "complex";
                break;
            case FUNC_MDA_DIVIDE:
                function = "divide";
                break;
            case FUNC_MDA_MODULO:
                function = "modulo";
                break;
            case FUNC_MDA_POW:
                function = "pow";
                break;
            case FUNC_MDA_SHIFT:
                function = "shift";
                break;
            case FUNC_MDA_EXTEND:
                function = "extend";
                break;
            case FUNC_MDA_SCALE:
                function = "scale";
                break;
            case FUNC_MDA_DIV:
                function = "div";
                break;
        }
        if (function != null) {
            String left = nodes[LEFT].getValue(session, false).toString();
            Type arrayType = nodes[LEFT].getDataType();
            if (arrayType instanceof MDAType) {
                arrayType = ((MDAType) arrayType).getDomain();
            }
            String right = nodes[RIGHT].getValue(session, arrayType, false).toString();
            final String functionCall = String.format("%s(%s, %s)", function, left, right);
            if (!isMDARootNode) {
                return functionCall;
            }
            return RasUtil.executeHsqlArrayQuery(functionCall, getRasArrayIds(session));
        }
        throw Error.runtimeError(ErrorCode.U_S0500, "Required: aggregate function. found: " + funcType);
    }
    
    private Object getLoHiValue(final Session session, boolean isMDARootNode) {
        Object ret = null;
        
        final String function;
        switch (funcType) {
            case FUNC_MDA_LO:
                function = Tokens.T_MDA_LO;
                break;
            case FUNC_MDA_HI:
                function = Tokens.T_MDA_HI;
                break;
            default:
                function = null;
                break;
        }
        if (function != null) {
            Integer index = null;
            if (nodes[RIGHT].getDataType().isCharacterType()) {
                String name = nodes[RIGHT].getValue(session, false).toString();
                index = getIndexForName(name);
            } else {
                index = (Integer) nodes[RIGHT].getValue(session, false);
            }
            String left = nodes[LEFT].getValue(session, false).toString();
            String rasql = String.format("sdom(%s)[%d].%s", left, index, function);
            if (isMDARootNode) {
                ret = RasUtil.executeHsqlArrayQuery(rasql, getRasArrayIds(session));
            } else {
                ret = rasql;
            }
        }
        return ret;
    }
    
    private Integer getIndexForName(String name) {
        Integer ret = null;
        Type arrayType = nodes[LEFT].getDataType();
        if (arrayType.isMDArrayType()) {
            MDADomainType domainType = ((MDAType) arrayType).getDomain();
            ret = domainType.getDimensionIndex(name);
            if (ret == MDADomainType.INVALID_DIMENSION_INDEX) {
                throw org.hsqldb.error.Error.error(ErrorCode.MDA_INVALID_PARAMETER,
                        "Dimension name not found: " + name);
            }
        }
        return ret;
    }
    
    private String getNameForIndex(int index) {
        String ret = null;
        Type arrayType = nodes[LEFT].getDataType();
        if (arrayType.isMDArrayType()) {
            MDADomainType domainType = ((MDAType) arrayType).getDomain();
            MDADimensionType dimensionType = domainType.getDimension(index);
            if (dimensionType == null) {
                throw org.hsqldb.error.Error.error(ErrorCode.MDA_INVALID_PARAMETER,
                        "Dimension index not found: " + index);
            }
            ret = dimensionType.getDimensionName();
        }
        return ret;
    }
    
    private Integer getDimensionality() {
        Integer ret = null;
        Type arrayType = nodes[LEFT].getDataType();
        if (arrayType.isMDArrayType()) {
            MDADomainType domainType = ((MDAType) arrayType).getDomain();
            ret = domainType.getDimensionality();
        }
        return ret;
    }
}
