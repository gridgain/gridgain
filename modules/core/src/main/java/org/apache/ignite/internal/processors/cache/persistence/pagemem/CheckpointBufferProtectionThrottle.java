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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
class CheckpointBufferProtectionThrottle {
    /**
     * Starting throttle time. Limits write speed to 1000 MB/s.
     */
    private static final long STARTING_THROTTLE_NANOS = 4000;

    /**
     * Backoff ratio. Each next park will be this times longer.
     */
    private static final double BACKOFF_RATIO = 1.05;

    /**
     * Exponential backoff counter.
     */
    private final AtomicInteger exponentialBackoffCntr = new AtomicInteger(0);

    long computeProtectionParkTime() {
        int exponent = exponentialBackoffCntr.getAndIncrement();
        return (long) (STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, exponent));
    }

    void resetExponentialBackoffCounter() {
        exponentialBackoffCntr.set(0);
    }
}
