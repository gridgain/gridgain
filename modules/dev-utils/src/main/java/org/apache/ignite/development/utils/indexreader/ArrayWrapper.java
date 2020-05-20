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

package org.apache.ignite.development.utils.indexreader;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.RandomAccess;
import org.jetbrains.annotations.Nullable;

/**
 * Array wrapper for automatically increasing size when inserting an element.
 * Only random insertion({@link #set}) and receiving({@link #get}) is supported.
 */
public class ArrayWrapper<E> extends AbstractList<E> implements RandomAccess {
    /** Array. */
    private Object[] arr = new Object[10];

    /** Largest index of inserted element into array. */
    private int maxInsertIdx = -1;

    /**
     * Inserting an element into array by index.
     * If index is larger than size of array,
     * than array is automatically increased by {@code 1.5 * (idx + 1)}.
     *
     * @param idx Array index.
     * @param element Insertable element.
     * @return Old element by index.
     * @throws ArrayIndexOutOfBoundsException If index is negative.
     */
    @Nullable @Override public E set(int idx, @Nullable E element) {
        if (idx < 0)
            throw new ArrayIndexOutOfBoundsException(idx);

        ensureCapacity(idx + 1);

        E oldElement = (E)arr[idx];
        arr[idx] = element;

        maxInsertIdx = Math.max(maxInsertIdx, idx);

        return oldElement;
    }

    /**
     * Returns an array element by index.
     *
     * @param idx Array index.
     * @return Array element.
     * @throws ArrayIndexOutOfBoundsException If index is outside array.
     */
    @Nullable @Override public E get(int idx) {
        return (E)arr[idx];
    }

    /**
     * Returns size of array, which is defined as largest index of inserted element {@code + 1}.
     *
     * @return Array size.
     */
    @Override public int size() {
        return maxInsertIdx + 1;
    }

    /**
     * Trim array to size.
     *
     * @return Current instance.
     */
    public ArrayWrapper<E> trimToSize() {
        arr = isEmpty() ? new Object[0] : Arrays.copyOf(arr, size());

        return this;
    }

    /**
     * Increasing array by {@code 1.5 * minCap} if it is less than {@code minCap}.
     *
     * @param minCap Minimum capacity.
     */
    private void ensureCapacity(int minCap) {
        if (minCap - arr.length <= 0)
            return;

        arr = Arrays.copyOf(arr, (int)(minCap * 1.5));
    }
}
