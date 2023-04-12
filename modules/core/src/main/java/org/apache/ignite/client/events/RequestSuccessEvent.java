/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.client.events;

import java.util.concurrent.TimeUnit;

/** */
public class RequestSuccessEvent extends RequestEvent {
    /** */
    private final long elapsedTimeNanos;

    /**
     * @param conn Connection description.
     * @param requestId Request id.
     * @param opCode Operation code.
     * @param opName Operation name.
     * @param elapsedTimeNanos Elapsed time in nanoseconds.
     */
    public RequestSuccessEvent(
        ConnectionDescription conn,
        long requestId,
        short opCode,
        String opName,
        long elapsedTimeNanos
    ) {
        super(conn, requestId, opCode, opName);

        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Get the elapsed time of the request.
     *
     * @param timeUnit Desired time unit in which to return the elapsed time.
     * @return The elapsed time.
     */
    public long elapsedTime(TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }
}
