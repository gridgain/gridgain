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

package org.apache.ignite.internal.processors.cache.checker.util;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Container for object that available after time.
 */
public class DelayedHolder<T> implements Delayed {
    /** Time of unavailability. */
    private final long finishTime;

    /** Keep the value. */
    private final T task;

    /**
     * @param time Time in millis.
     * @param task Task.
     */
    public DelayedHolder(long time, T task) {
        this.finishTime = time;
        this.task = task;
    }

    /**
     * @return task.
     */
    public T getTask() {
        return task;
    }

    /**
     * @return remanding delay in millis.
     */
    @Override public long getDelay(@NotNull TimeUnit unit) {
        return unit.convert(finishTime - U.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Compare objects by finishTime.
     */
    @Override public int compareTo(@NotNull Delayed o) {
        return Long.compare(finishTime, ((DelayedHolder)o).finishTime);
    }
}
