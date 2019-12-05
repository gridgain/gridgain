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
package org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask;

import java.io.Serializable;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Pending task for single node. It should be used to store information about long operations (e.g. index deletion)
 * for case when node with persistence fails before operation is completed. After start, node reads it's
 * pending tasks from metastorage and completes them.
 */
public abstract class AbstractNodePendingTask implements Serializable {
    /**
     * Pending task can change the configuration of persistent cache. In this case in this field should be stored
     * changed {@link StoredCacheData}, which is used to send cache discovery information through cluster
     * after node start, but before pending task is completed.
     */
    protected StoredCacheData changedCacheData;

    /** */
    protected AbstractNodePendingTask() {
        /* No op. */
    }

    /**
     * @return Changed cache data.
     */
    public StoredCacheData changedCacheData() {
        return changedCacheData;
    }

    /**
     * Short name of pending task is used to build metastorage key for saving this task.
     * @return Short name of this task.
     */
    public abstract String shortName();

    /**
     * Method that executes the task. It is called after node start.
     * @param ctx Grid kernal context.
     */
    public abstract void execute(GridKernalContext ctx);

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(AbstractNodePendingTask.class, this);
    }
}
