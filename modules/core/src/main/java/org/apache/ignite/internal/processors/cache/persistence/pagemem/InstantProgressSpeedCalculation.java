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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

/**
 * Represents instant speed calculation of some progress starting from zero.
 */
class InstantProgressSpeedCalculation {
    /**
     * Measurement used to calculate average speed. History recording is disabled.
     */
    private final IntervalBasedMeasurement measurement = new IntervalBasedMeasurement();

    /**
     * Sets the value which was reached by the progress.
     *
     * @param progress  progress value
     * @param nanoTime  time instant
     */
    public void setProgress(long progress, long nanoTime) {
        measurement.setCounter(progress, nanoTime);
    }

    /**
     * Returns speed of progress in operations per second.
     *
     * @param nanoTime time instant at which the speed is to be calculated
     * @return ops per second
     */
    public long getOpsPerSecond(long nanoTime) {
        return measurement.getSpeedOpsPerSec(nanoTime);
    }

    /**
     * Returns speed of progress in operations per second as calculated for the 'now' instant.
     *
     * @return ops per second
     */
    public long getOpsPerSecondAtNow() {
        return measurement.getSpeedOpsPerSecReadOnly();
    }

    /**
     * Stops updates, finalizes its internals.
     */
    public void stop() {
        measurement.finishInterval();
    }
}
