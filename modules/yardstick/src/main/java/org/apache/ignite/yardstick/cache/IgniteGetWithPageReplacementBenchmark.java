/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.yardstick.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Benchmark get operation with page replacement.
 */
public class IgniteGetWithPageReplacementBenchmark extends IgniteAbstractPageReplacementBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        int portion = 100;

        Set<Integer> getSet = new HashSet<>(portion, 1.f);

        for (int i = 0; i < portion; i++)
            getSet.add(nextKey());

        cache().getAll(getSet);

        return true;
    }
}
