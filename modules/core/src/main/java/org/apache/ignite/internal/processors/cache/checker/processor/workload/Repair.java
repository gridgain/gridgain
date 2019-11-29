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
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.processor.PipelineWorkload;
import org.apache.ignite.internal.util.typedef.T2;

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
    private Map<UUID, T2<KeyCacheObject, CacheObject>> data;

    /**
     *
     */
    private long recheckUpdateCounter;

    /**
     *
     */
    private long recheckStartTime;

    /** */
    public Repair(String cacheName,
        int partId,
        Map<UUID, T2<KeyCacheObject, CacheObject>> data,
        long recheckUpdateCntr,
        long recheckStartTime
    ) {
        this.cacheName = cacheName;
        this.partId = partId;
        this.data = data;
        this.recheckUpdateCounter = recheckUpdateCntr;
        this.recheckStartTime = recheckStartTime;
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
    public Map<UUID, T2<KeyCacheObject, CacheObject>> getData() {
        return data;
    }

    /**
     *
     */
    public long getRecheckUpdateCounter() {
        return recheckUpdateCounter;
    }

    /**
     *
     */
    public long getRecheckStartTime() {
        return recheckStartTime;
    }
}
