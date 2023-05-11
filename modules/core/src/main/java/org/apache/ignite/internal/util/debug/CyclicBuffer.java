/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util.debug;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public class CyclicBuffer<T> {
    private long size = 0;

    private final T[] buffer;

    public CyclicBuffer(int size) {
        buffer = (T[])new Object[size];
    }

    public void add(T t) {
        buffer[(int)(size++ % buffer.length)] = t;
    }

    public Stream<T> stream() {
        return Arrays.stream(buffer)
            .filter(Objects::nonNull);
    }
}
