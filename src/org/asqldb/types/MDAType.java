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

import java.sql.Array;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.SortAndSlice;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCArray;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Class for MDARRAY type objects (multidimensional arrays). An MDARRAY has a
 * cell type, and a definition domain (bounding box)
 * .<p>
 *
 * Grammar:
 * <pre>
 * <array type> ::=
 * <data type> MDARRAY [ <array definition domain> ]
 * </pre>
 *
 * @author Dimitar Misev
 */
public class MDAType extends Type {

    final Type dataType;
    final MDADomainType domain;

    public MDAType(Type dataType) {
        this(dataType, new MDADomainType());
    }

    public MDAType(Type dataType, MDADomainType domain) {

        super(Types.SQL_MDARRAY, Types.SQL_MDARRAY, 0, 0);

        if (dataType == null) {
            dataType = Type.SQL_ALL_TYPES;
        }

        this.dataType = dataType;
        this.domain = domain;
    }

    public String getRasqlCollectionType() {
        String ret = "";

        String rasqlBaseType = dataType.getRasqlType();
        if (rasqlBaseType == null) {
            throw new RuntimeException("Unsupported array base type.");
        } else if (rasqlBaseType.equals("bool")) {
            ret = "BoolSet";
        } else if (rasqlBaseType.equals("char")) {
            ret = "GreySet";
        } else if (rasqlBaseType.equals("short")) {
            ret = "ShortSet";
        } else if (rasqlBaseType.equals("long")) {
            ret = "LongSet";
        } else if (rasqlBaseType.equals("float")) {
            ret = "FloatSet";
        } else if (rasqlBaseType.equals("double")) {
            ret = "DoubleSet";
        }

        int dimensionality = domain.getDimensionality();
        if (dimensionality > 3) {
            // TODO: once possible to construct types on the fly, update
            throw new RuntimeException("Arrays with more than 3 dimensions are not supported yet.");
        }
        if (dimensionality != 2) {
            ret += dimensionality;
        }

        return ret;
    }

    @Override
    public int displaySize() {
        return 7 + (dataType.displaySize() + 1);
    }

    @Override
    public int getJDBCTypeCode() {
        return Types.ARRAY;
    }

    @Override
    public Class getJDBCClass() {
        return java.sql.Array.class;
    }

    @Override
    public String getJDBCClassName() {
        return "java.sql.Array";
    }

    @Override
    public int getJDBCScale() {
        return 0;
    }

    @Override
    public int getJDBCPrecision() {
        return 0;
    }

    @Override
    public int getSQLGenericTypeCode() {
        return 0;
    }

    public MDADomainType getDomain() {
        return domain;
    }

    @Override
    public String getNameString() {
        StringBuffer sb = new StringBuffer();

        sb.append(dataType.getNameString()).append(' ');
        sb.append(Tokens.T_MDARRAY);
        sb.append(domain.toString());

        return sb.toString();
    }

    @Override
    public String getFullNameString() {
        StringBuffer sb = new StringBuffer();

        sb.append(dataType.getFullNameString()).append(' ');
        sb.append(Tokens.T_MDARRAY);
        sb.append(domain.toString());

        return sb.toString();
    }

    @Override
    public String getDefinition() {
        StringBuffer sb = new StringBuffer();

        sb.append(dataType.getDefinition()).append(' ');
        sb.append(Tokens.T_MDARRAY);
        sb.append(domain.toString());

        return sb.toString();
    }

    /**
     * @TODO
     */
    @Override
    public int compare(Session session, Object a, Object b) {
        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        Object[] arra = (Object[]) a;
        Object[] arrb = (Object[]) b;
        int length = arra.length;

        if (arrb.length < length) {
            length = arrb.length;
        }

        for (int i = 0; i < length; i++) {
            int result = dataType.compare(session, arra[i], arrb[i]);

            if (result != 0) {
                return result;
            }
        }

        if (arra.length > arrb.length) {
            return 1;
        } else if (arra.length < arrb.length) {
            return -1;
        }

        return 0;
    }

    /**
     * @TODO
     */
    @Override
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        if (a == null) {
            return null;
        }
        return a;

//        Object[] arra = (Object[]) a;
//
//        if (arra.length > domain.getCardinality()) {
//            throw Error.error(ErrorCode.X_2202F);
//        }
//
//        Object[] arrb = new Object[arra.length];
//
//        for (int i = 0; i < arra.length; i++) {
//            arrb[i] = dataType.convertToTypeLimits(session, arra[i]);
//        }
//
//        return arrb;
    }

    /**
     * @TODO
     */
    @Override
    public Object convertToType(SessionInterface session, Object a,
            Type otherType) {

        if (a == null) {
            return null;
        }

        if (otherType == null) {
            return a;
        }

//        if (!otherType.isMDArrayType()) {
//            throw Error.error(ErrorCode.X_42562);
//        }

//        Object[] arra = (Object[]) a;
//
//        if (arra.length > domain.getCardinality()) {
//            throw Error.error(ErrorCode.X_2202F);
//        }
//
//        Type otherComponent = otherType.collectionBaseType();
//
//        if (dataType.equals(otherComponent)) {
//            return a;
//        }
//
//        Object[] arrb = new Object[arra.length];
//
//        for (int i = 0; i < arra.length; i++) {
//            arrb[i] = dataType.convertToType(session, arra[i], otherComponent);
//        }
//
//        return arrb;
        return a;
    }

    /**
     * @TODO
     */
    @Override
    public Object convertJavaToSQL(SessionInterface session, Object a) {

        Object[] data;
        boolean convert = false;

        if (a == null) {
            return null;
        }

        if (a instanceof Object[]) {
            data = (Object[]) a;
            convert = true;
        } else if (a instanceof JDBCArray) {
            data = ((JDBCArray) a).getArrayInternal();
        } else if (a instanceof JDBCArrayBasic) {
            data = (Object[]) ((JDBCArrayBasic) a).getArray();
            convert = true;
        } else if (a instanceof java.sql.Array) {
            try {
                data = (Object[]) ((Array) a).getArray();
                convert = true;
            } catch (Exception e) {
                throw Error.error(ErrorCode.X_42561);
            }
        } else {
            throw Error.error(ErrorCode.X_42561);
        }

        if (convert) {
            Object[] array = new Object[data.length];

            for (int i = 0; i < data.length; i++) {
                array[i] = dataType.convertJavaToSQL(session, data[i]);
                array[i] = dataType.convertToTypeLimits(session, data[i]);
            }

            return array;
        }

        return data;
    }

    /**
     * @TODO
     */
    @Override
    public Object convertSQLToJava(SessionInterface session, Object a) {

        if (a instanceof Object[]) {
            Object[] data = (Object[]) a;

            return new JDBCArray(data, this.collectionBaseType(), this,
                    session);
        }

        throw Error.error(ErrorCode.X_42561);
    }

    @Override
    public Object convertToDefaultType(SessionInterface sessionInterface,
            Object o) {
        return o;
    }

    @Override
    public String convertToString(Object a) {
        if (a == null) {
            return null;
        }

        return convertToSQLString(a);
    }

    /**
     * @TODO
     */
    @Override
    public String convertToSQLString(Object a) {
        if (a == null) {
            return Tokens.T_NULL;
        }

        Object[] arra = (Object[]) a;
        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_MDARRAY);
        sb.append('[');

        for (int i = 0; i < arra.length; i++) {
            if (i > 0) {
                sb.append(',');
            }

            sb.append(dataType.convertToSQLString(arra[i]));
        }

        sb.append(']');

        return sb.toString();
    }

    /**
     * @TODO
     */
    public boolean canConvertFrom(Type otherType) {
        if (otherType == null) {
            return true;
        }

        if (!otherType.isMDArrayType()) {
            return false;
        }

        Type otherComponent = otherType.collectionBaseType();

        return dataType.canConvertFrom(otherComponent);
    }

    /**
     * @TODO
     */
    @Override
    public int canMoveFrom(Type otherType) {
        if (otherType == this) {
            return 0;
        }

        if (!otherType.isMDArrayType()) {
            return -1;
        }

        if (domain.getCardinality() >= ((MDAType) otherType).domain.getCardinality()) {
            return dataType.canMoveFrom((MDAType) otherType);
        } else {
            if (dataType.canMoveFrom((MDAType) otherType) == -1) {
                return -1;
            }

            return 1;
        }
    }

    /**
     * @TODO
     */
    @Override
    public boolean canBeAssignedFrom(Type otherType) {

        if (otherType == null) {
            return true;
        }

        Type otherComponent = otherType.collectionBaseType();

        return otherComponent != null
                && dataType.canBeAssignedFrom(otherComponent);
    }

    @Override
    public Type collectionBaseType() {
        return dataType;
    }

    @Override
    public int arrayLimitCardinality() {
        return (int) domain.getCardinality();
    }

    @Override
    public boolean isMDArrayType() {
        return true;
    }

    @Override
    public Type getAggregateType(Type other) {
        if (other == null) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        if (this == other) {
            return this;
        }

        if (!other.isMDArrayType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        Type otherComponent = other.collectionBaseType();

        if (dataType.equals(otherComponent)) {
            return ((MDAType) other).domain.getCardinality() > domain.getCardinality() ? other
                    : this;
        }

        Type newComponent = dataType.getAggregateType(otherComponent);
        MDADomainType newDomain = ((MDAType) other).domain.getCardinality() > domain.getCardinality()
                ? ((MDAType) other).domain
                : domain;

        return new MDAType(newComponent, newDomain);
    }

    @Override
    public Type getCombinedType(Session session, Type other, int operation) {
        MDAType type = (MDAType) getAggregateType(other);

        if (other == null) {
            return type;
        }

        if (operation != OpTypes.CONCAT) {
            return type;
        }

        return new MDAType(dataType);
    }

    /**
     * @TODO
     */
    @Override
    public int cardinality(Session session, Object a) {
        if (a == null) {
            return 0;
        }

        return ((Object[]) a).length;
    }

    /**
     * @TODO
     */
    @Override
    public Object concat(Session session, Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }

        int size = ((Object[]) a).length + ((Object[]) b).length;
        Object[] array = new Object[size];

        System.arraycopy(a, 0, array, 0, ((Object[]) a).length);
        System.arraycopy(b, 0, array, ((Object[]) a).length,
                ((Object[]) b).length);

        return array;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other instanceof Type) {
            if (((Type) other).typeCode != Types.SQL_ARRAY) {
                return false;
            }

            return domain.getCardinality() == ((MDAType) other).domain.getCardinality()
                    && dataType.equals(((MDAType) other).dataType);
        }

        return false;
    }

    /**
     * @TODO
     */
    @Override
    public int hashCode(Object a) {
        if (a == null) {
            return 0;
        }

        int hash = 0;
        Object[] array = (Object[]) a;

        for (int i = 0; i < array.length && i < 4; i++) {
            hash += dataType.hashCode(array[i]);
        }

        return hash;
    }

    /**
     * @TODO
     */
    public void sort(Session session, Object a, SortAndSlice sort) {
        Object[] array = (Object[]) a;
        TypedComparator comparator = session.getComparator();

        comparator.setType(dataType, sort);
        ArraySort.sort(array, 0, array.length, comparator);
    }

    /**
     * @TODO
     */
    public int deDuplicate(Session session, Object a, SortAndSlice sort) {
        Object[] array = (Object[]) a;
        TypedComparator comparator = session.getComparator();

        comparator.setType(dataType, sort);

        return ArraySort.deDuplicate(array, 0, array.length, comparator);
    }

    @Override
    public String toString() {
        return dataType.getDefinition() + " MDARRAY " + domain;
    }
}
