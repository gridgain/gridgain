/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.binary.nextgen;

import java.util.function.IntUnaryOperator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BikeTupleTest {
    static class TestTuple extends BikeTuple {
        TestTuple(byte[] data) {
            super(data);
        }

        public String strAttr(int i) {
            return (String)attr(i, String.class, intType(String.class));
        }

        public int intAttr(int i) {
            return (Integer)attr(i, Integer.class, intType(Integer.class));
        }

        public long longAttr(int i) {
            return (Long)attr(i, Long.class, intType(Long.class));
        }
    }

    @Test
    public void singleVarlenNonNull2() throws Exception {
        byte[] bytes = new BikeBuilder(1)
                .append("A")
                .build();

        TestTuple t = new TestTuple(bytes);

        assertEquals("A", t.strAttr(0));
    }

    @Test
    public void varlenThenNull2() throws Exception {
        byte[] bytes = new BikeBuilder(2)
                .append("A")
                .append(null)
                .build();

        TestTuple tuple = new TestTuple(bytes);

        assertEquals("A", tuple.strAttr(0));
        assertNull(tuple.strAttr(1));
    }

    @Test
    public void nullThenVarlen2() throws Exception {
        byte[] bytes = new BikeBuilder(2)
                .append(null)
                .append("A")
                .build();

        TestTuple tuple = new TestTuple(bytes);

        assertNull(tuple.strAttr(0));
        assertEquals("A", tuple.strAttr(1));
    }

    @Test
    public void varlenNullVarlen() throws Exception {
        byte[] bytes = new BikeBuilder(3)
            .append("A")
            .append(null)
            .append(1)
            .build();

        TestTuple tuple = new TestTuple(bytes);

        assertEquals("A", tuple.strAttr(0));
        assertNull(tuple.strAttr(1));
        assertEquals(1, tuple.intAttr(2));
    }

    private static IntUnaryOperator lenSchema(int... lens) {
        return i -> lens[i];
    }
}
