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
public class HandshakeFailEvent extends ConnectionEvent {
    /** */
    private final long elapsedTimeNanos;

    /** */
    private final Throwable throwable;

    /**
     * @param conn Connection description.
     * @param elapsedTimeNanos Elapsed time in nanoseconds.
     * @param throwable Throwable that caused the failure.
     */
    public HandshakeFailEvent(
        ConnectionDescription conn,
        long elapsedTimeNanos,
        Throwable throwable
    ) {
        super(conn);

        this.elapsedTimeNanos = elapsedTimeNanos;
        this.throwable = throwable;
    }

    /**
     * Get the elapsed time of the handshake.
     *
     * @param timeUnit Desired time unit in which to return the elapsed time.
     * @return the elapsed time.
     */
    public long elapsedTime(TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Get a cause of the failure.
     *
     * @return a cause of the failure.
     */
    public Throwable throwable() {
        return throwable;
    }
}
