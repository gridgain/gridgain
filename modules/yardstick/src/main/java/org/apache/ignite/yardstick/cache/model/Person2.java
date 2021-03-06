/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.cache.model;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Value used for indexed put test.
 */
public class Person2 implements Serializable {
    /** Value 1. */
    @QuerySqlField(index = true)
    private int val1;

    /** Value 2. */
    @QuerySqlField(index = true)
    private int val2;

    /**
     * Constructs.
     *
     * @param val Value.
     */
    public Person2(int val) {
        this.val1 = val;
        this.val2 = val + 1;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Person2 value2 = (Person2)o;

        return val1 == value2.val1 && val2 == value2.val2;
    }

    /** */
    public int val1() {
        return val1;
    }

    /** */
    public int val2() {
        return val2;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * val1 + val2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person2 [val1=" + val1 + ", val2=" + val2 + ']';
    }
}