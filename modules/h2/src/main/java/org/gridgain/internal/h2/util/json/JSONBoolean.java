/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.util.json;

/**
 * JSON boolean.
 */
public class JSONBoolean extends JSONValue {

    /**
     * {@code false} value.
     */
    public static final JSONBoolean FALSE = new JSONBoolean(false);

    /**
     * {@code true} value.
     */
    public static final JSONBoolean TRUE = new JSONBoolean(true);

    private final boolean value;

    private JSONBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public void addTo(JSONTarget<?> target) {
        if (value) {
            target.valueTrue();
        } else {
            target.valueFalse();
        }
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public boolean getBoolean() {
        return value;
    }

}
