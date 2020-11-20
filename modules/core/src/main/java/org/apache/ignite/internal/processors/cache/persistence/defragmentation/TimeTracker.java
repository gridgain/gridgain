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
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class TimeTracker<Stage extends Enum<Stage>> {
    /** */
    private final Class<Stage> stageCls;

    /** */
    private final long[] t;

    /** */
    private long lastStage;

    /** */
    public TimeTracker(Class<Stage> stageCls) {
        this.stageCls = stageCls;

        t = new long[stageCls.getEnumConstants().length];

        lastStage = System.nanoTime();
    }

    /** */
    public void complete(Stage stage) {
        int ordinal = stage.ordinal();

        if (ordinal == 0) {
            Arrays.fill(t, 0L);

            lastStage = System.nanoTime();
        }
        else {
            long now = System.nanoTime();

            t[ordinal] += (now - lastStage);

            lastStage = now;
        }
    }

    /** */
    public long getMillis(Stage stage) {
        return U.nanosToMillis(t[stage.ordinal()]);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder("[");

        for (Stage s : stageCls.getEnumConstants()) {
            sb.append(s.name().toLowerCase()).append("=")
                .append(U.nanosToMillis(t[s.ordinal()]) * 1e-3).append("s, ");
        }

        sb.setLength(sb.length() - 2);

        sb.append("]");

        return sb.toString();
    }
}
