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

package org.apache.ignite.internal.processors.localtask;

import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;

/**
 * The task to be convertible after restoring from metaStorage.
 */
class ConvertibleTask extends SimpleTask {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public ConvertibleTask() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param name Task name.
     */
    public ConvertibleTask(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override public DurableBackgroundTask<?> convertAfterRestoreIfNeeded() {
        return new SimpleTask("converted-task-" + name());
    }
}
