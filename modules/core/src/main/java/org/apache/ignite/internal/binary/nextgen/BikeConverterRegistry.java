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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryObjectImpl;

public class BikeConverterRegistry {
    private static final boolean bikeRowFormat = Boolean.getBoolean("bike.row.format");

    private static final ConcurrentHashMap<Object, Function<BinaryObject, BikeTuple>> converterMap =
        new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Object, Function<BikeTuple, BinaryObjectImpl>> backConverterMap =
        new ConcurrentHashMap<>();

    public static final ThreadLocal<Boolean> queryApi = ThreadLocal.withInitial(() -> false);

    public static void registerConverter(Object key, Function<BinaryObject, BikeTuple> converter) {
        converterMap.put(key, converter);
    }

    public static Function<BinaryObject, BikeTuple> converter(Object key) {
        return bikeRowFormat ? converterMap.get(key) : null;
    }

    public static void registerBackConverter(Object key, Function<BikeTuple, BinaryObjectImpl> converter) {
        backConverterMap.put(key, converter);
    }

    public static Function<BikeTuple, BinaryObjectImpl> backConverter(Object key) {
        return backConverterMap.get(key);
    }

    public static class Stat {
        private final String label;
        private final AtomicLong sum = new AtomicLong();
        private final AtomicLong cnt = new AtomicLong();
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);

        public Stat(String label) {
            this.label = label;
        }

        public void appendStat(int sz) {
            cnt.incrementAndGet();
            sum.addAndGet(sz);
            max.updateAndGet(max -> sz > max ? sz : max);
            min.updateAndGet(min -> sz < min ? sz : min);
        }

        public void dump() {
            System.err.println(label + " statistics:");
            System.err.println("MIN: " + min.get());
            System.err.println("MAX: " + max.get());
            System.err.println("AVG: " + (float)sum.get() / cnt.get());
        }
    }

    public static final Stat bikeStat = new Stat("BIKE");
    public static final Stat binStat = new Stat("BINARY OBJECT");
}
