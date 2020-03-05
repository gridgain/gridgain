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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.Serializable;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Container for {@link CacheObject} with version.
 */
public class VersionedValue implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Value. */
    private CacheObject val;

    /** Version. */
    private GridCacheVersion ver;

    /**
     * Partition update counter.
     */
    private long updateCntr;

    /**
     * Recheck start time.
     */
    private long recheckStartTime;

    /**
     * @param val Value.
     * @param ver Version.
     * @param updateCntr Update counter.
     * @param recheckStartTime Recheck start time.
     */
    public VersionedValue(CacheObject val, GridCacheVersion ver, long updateCntr, long recheckStartTime) {
        this.val = val;
        this.ver = ver;
        this.updateCntr = updateCntr;
        this.recheckStartTime = recheckStartTime;
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updateCntr;
    }

    /**
     * @return Recheck start time.
     */
    public long recheckStartTime() {
        return recheckStartTime;
    }
}
