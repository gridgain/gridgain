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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ConcurrentCyclicBuffer<T> {
    private final AtomicLong size = new AtomicLong();

    private final AtomicReferenceArray<T> buffer;

    public ConcurrentCyclicBuffer(int size) {
        buffer = new AtomicReferenceArray<>(size);
    }

    public void add(T t) {
        buffer.set((int)(size.getAndIncrement() % buffer.length()), t);
    }

    public Stream<T> stream() {
        return IntStream.range(0, buffer.length())
            .mapToObj(buffer::get)
            .filter(Objects::nonNull);
    }
}
