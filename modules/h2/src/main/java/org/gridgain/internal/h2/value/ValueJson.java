/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Lazarev Nikita <lazarevn@ispras.ru>
 */
package org.gridgain.internal.h2.value;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.gridgain.internal.h2.api.ErrorCode;
import org.gridgain.internal.h2.message.DbException;
import org.gridgain.internal.h2.util.StringUtils;
import org.gridgain.internal.h2.util.json.JSONStringSource;

/**
 * Implementation of the JSON data type.
 */
public class ValueJson extends Value {

    private String value;

    private ValueJson(String value) {
        this.value = value;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        return StringUtils.quoteStringSQL(builder, value).append("::JSON");
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_JSON;
    }

    @Override
    public int getValueType() {
        return Value.JSON;
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public int getMemory() {
        return value.length() * 2 + 94;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setString(parameterIndex, value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueJson && value.equals(((ValueJson) other).value);
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        String other = ((ValueJson) v).value;
        return mode.compareString(value, other, false);
    }

    public static ValueJson get(String s) {
        try {
            s = JSONStringSource.normalize(s);
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
        return getInternal(s);
    }

    public static ValueJson get(byte[] bytes) {
        String s;
        try {
            s = JSONStringSource.normalize(bytes);
        } catch (RuntimeException ex) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, StringUtils.convertBytesToHex(bytes));
        }
        return getInternal(s);
    }

    public static ValueJson get(int number) {
        return getInternal(Integer.toString(number));
    }

    public static ValueJson get(long number) {
        return getInternal(Long.toString(number));
    }

    public static ValueJson get(BigDecimal number) {
        return getInternal(number.toString());
    }

    private static ValueJson getInternal(String s) {
        return new ValueJson(s);
    }

}
