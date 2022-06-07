/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * File I/O that emulates poor checkpoint metadata write speed.
 */
public class SlowCheckpointMetadataFileIOFactory extends AbstractSlowCheckpointFileIOFactory {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * @param slowCheckpointEnabled Slow checkpoint enabled.
     * @param checkpointParkNanos Checkpoint park nanos.
     */
    public SlowCheckpointMetadataFileIOFactory(AtomicBoolean slowCheckpointEnabled, long checkpointParkNanos) {
        super(slowCheckpointEnabled, checkpointParkNanos);
    }

    /** {@inheritDoc} */
    @Override protected boolean shouldSlowDownCurrentThread() {
        return Thread.currentThread().getName().contains("db-checkpoint-thread");
    }
}
