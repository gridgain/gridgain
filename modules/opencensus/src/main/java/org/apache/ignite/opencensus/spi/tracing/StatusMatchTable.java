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

package org.apache.ignite.opencensus.spi.tracing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.Status;

/**
 * Table to match OpenCensus span statuses with declated on Tracing SPI.
 */
public class StatusMatchTable {
    /** Table. */
    private static final Map<Status, io.opencensus.trace.Status> table = new ConcurrentHashMap<>();

    static {
        table.put(Status.OK, io.opencensus.trace.Status.OK);
        table.put(Status.CANCELLED, io.opencensus.trace.Status.CANCELLED);
        table.put(Status.ABORTED, io.opencensus.trace.Status.ABORTED);
    }

    /**
     * Default constructor.
     */
    private StatusMatchTable() {
        //
    }

    /**
     * @param status Status.
     */
    public static io.opencensus.trace.Status match(Status status) {
        io.opencensus.trace.Status result = table.get(status);

        if (result == null)
            throw new IgniteException("Unknown status (no matching with opencensus): " + status);

        return result;
    }
}
