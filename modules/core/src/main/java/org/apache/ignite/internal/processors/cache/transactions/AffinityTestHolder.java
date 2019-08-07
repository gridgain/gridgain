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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;

public class AffinityTestHolder {
    public static ConcurrentMap<String, GridCacheAffinityManager> cacheAffinityManagers = new ConcurrentHashMap<>();

    public static ConcurrentMap<String, AtomicBoolean> printer = new ConcurrentHashMap<>();

    public static AtomicReference<Integer> movingPartitionId = new AtomicReference<>();


}
