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

package org.apache.ignite.ml.math.primitives.vector;

import org.apache.ignite.ml.math.primitives.vector.impl.DelegatingVector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class DelegatingVectorConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    @Test
    public void basicTest() {
        final Vector parent = new DenseVector(new double[] {0, 1});

        final DelegatingVector delegate = new DelegatingVector(parent);

        final int size = parent.size();

        assertEquals("Delegate size differs from expected.", size, delegate.size());

        assertEquals("Delegate vector differs from expected.", parent, delegate.getVector());

        for (int idx = 0; idx < size; idx++)
            assertDelegate(parent, delegate, idx);
    }

    /** */
    private void assertDelegate(Vector parent, Vector delegate, int idx) {
        assertValue(parent, delegate, idx);

        parent.set(idx, parent.get(idx) + 1);

        assertValue(parent, delegate, idx);

        delegate.set(idx, delegate.get(idx) + 2);

        assertValue(parent, delegate, idx);
    }

    /** */
    private void assertValue(Vector parent, Vector delegate, int idx) {
        assertEquals("Unexpected value at index " + idx, parent.get(idx), delegate.get(idx), 0d);
    }
}
