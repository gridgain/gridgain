/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.mxbean.DefragmentationMXBean;

/**
 * Defragmentation MX bean implementation.
 */
public class DefragmentationMXBeanImpl implements DefragmentationMXBean {
    /** Defragmentation manager. */
    private final IgniteDefragmentation defragmentation;

    public DefragmentationMXBeanImpl(GridKernalContext ctx) {
        this.defragmentation = ctx.defragmentation();
    }

    /** {@inheritDoc}
     * @return*/
    @Override public boolean schedule(String cacheNames) {
        final List<String> caches = Arrays.stream(cacheNames.split(","))
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());

        try {
            defragmentation.schedule(caches);

            return true;
        }
        catch (IgniteCheckedException e) {
            return false;
        }
    }

    /** {@inheritDoc}
     * @return*/
    @Override public boolean cancel() {
        try {
            defragmentation.cancel();

            return true;
        }
        catch (IgniteCheckedException e) {
            return false;
        }
    }

    @Override public boolean inProgress() {
        return defragmentation.inProgress();
    }

    @Override public int processedPartitions() {
        return defragmentation.processedPartitions();
    }

    @Override public int totalPartitions() {
        return defragmentation.totalPartitions();
    }

    @Override public long startTime() {
        return defragmentation.startTime();
    }
}
