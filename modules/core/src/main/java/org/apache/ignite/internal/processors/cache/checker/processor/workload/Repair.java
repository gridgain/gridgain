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

package org.apache.ignite.internal.processors.cache.checker.processor.workload;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;

/**
 *
 */
public class Repair implements PipelineWorkload {
    /**
     *
     */
    private String cacheName;

    /**
     *
     */
    private int partId;

    /**
     *
     */
    private Map<KeyCacheObject, Map<UUID, VersionedValue>> data;

    /** */
    public Repair(String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> data
    ) {
        this.cacheName = cacheName;
        this.partId = partId;
        this.data = data;
    }

    /**
     *
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     *
     */
    public int getPartId() {
        return partId;
    }

    /**
     *
     */
    public Map<KeyCacheObject, Map<UUID, VersionedValue>> getData() {
        return data;
    }
}
