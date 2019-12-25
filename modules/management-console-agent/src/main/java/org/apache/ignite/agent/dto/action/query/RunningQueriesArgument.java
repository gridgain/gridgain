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

package org.apache.ignite.agent.dto.action.query;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Running queries argument.
 */
public class RunningQueriesArgument {
    /** Duration. */
    private long duration;

    /**
     * @return Duration.
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @param duration Duration.
     * @return {@code This} for chaining method calls.
     */
    public RunningQueriesArgument setDuration(long duration) {
        this.duration = duration;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningQueriesArgument.class, this);
    }
}
