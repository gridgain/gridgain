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

package org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask;

import java.io.Serializable;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;

/**
 * Durable task that should be used to do long operations (e.g. index deletion) in background.
 * @param <R> Type of the result of the task.
 */
public interface DurableBackgroundTask<R> extends Serializable {
    /**
     * Getting the name of the task to identify it.
     * Also used as part of a key for storage in a MetaStorage.
     *
     * @return Task name.
     */
    String name();

    /**
     * Canceling the task.
     */
    void cancel();

    /**
     * Asynchronous task execution.
     *
     * Completion of the task execution should be only with the {@link DurableBackgroundTaskResult result}.
     *
     * @param ctx Kernal context.
     * @return Future of the tasks.
     */
    IgniteInternalFuture<DurableBackgroundTaskResult<R>> executeAsync(GridKernalContext ctx);

    /**
     * Converting the current task to another after restoring from metaStorage.
     *
     * @return Converted task.
     */
    default DurableBackgroundTask<?> convertAfterRestoreIfNeeded() {
        return this;
    }
}
