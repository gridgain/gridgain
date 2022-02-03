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

package org.apache.ignite.internal.processors.compute;

import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Job that does nothing.
 */
class NoopJob extends ComputeJobAdapter {
    /** Sleep time in ms. */
    final long sleep;

    /**
     * Default constructor.
     */
    public NoopJob() {
        this(0);
    }

    /**
     * Constructor.
     *
     * @param sleep Sleep time in ms.
     */
    public NoopJob(long sleep) {
        this.sleep = sleep;
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        if (sleep > 0) {
            try {
                U.sleep(sleep);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }
        }

        return null;
    }
}
