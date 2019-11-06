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

package org.apache.ignite.agent.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

/**
 * Thread factory for management console threads.
 */
public class ManagementConsoleThreadFactory implements ThreadFactory {
    /** Thread index counter. */
    private final AtomicInteger threadIdx = new AtomicInteger();

    /** Thread name. */
    private final String threadName;

    /**
     * @param threadName Thread name.
     */
    public ManagementConsoleThreadFactory(String threadName) {
        this.threadName = threadName;
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        return new Thread(r, String.format("mc-%s-thread-%s", threadName, threadIdx.incrementAndGet()));
    }
}
