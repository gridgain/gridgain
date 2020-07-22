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
package org.apache.ignite.mxbean;

/**
 * This MX bean is used to configure diagnostic parameters.
 */
@MXBeanDescription("This MX bean is used to configure diagnostic parameters.")
public interface DiagnosticMXBean {
    /**
     * Shows if diagnotic message statistics is enabled.
     *
     * @return Whether enabled.
     */
    @MXBeanDescription("Shows if diagnotic message statistics is enabled.")
    public boolean getDiagnosticMessageStatsEnabled();

    /**
     * Enables or diables diagnotic message statistics.
     *
     * @param enabled Whether to enable.
     */
    @MXBeanDescription("Enables or diables diagnotic message statistics.")
    public void setDiagnosticMessageStatsEnabled(boolean enabled);

    /**
     * Returns time threshold for messages to consider them as long processed.
     *
     * @return Threshold in milliseconds.
     */
    @MXBeanDescription("Returns time threshold for messages to consider them as long processed.")
    public long getDiagnosticMessageStatTooLongProcessing();

    /**
     * Sets the time threshold for messages to consider them as long processed.
     *
     * @param val Threshold in milliseconds.
     */
    @MXBeanDescription("Sets the time threshold for messages to consider them as long processed.")
    public void setDiagnosticMessageStatTooLongProcessing(long val);

    /**
     * Returns time threshold for messages to consider them as long waiting in queue for procesing.
     *
     * @return Threshold in milliseconds.
     */
    @MXBeanDescription("Returns time threshold for messages to consider them as long waiting in queue for procesing.")
    public long getDiagnosticMessageStatTooLongWaiting();

    /**
     * Sets the time threshold for messages to consider them as long waiting in queue for procesing.
     *
     * @param val Threshold in milliseconds.
     */
    @MXBeanDescription("Sets the time threshold for messages to consider them as long waiting in queue for procesing.")
    public void setDiagnosticMessageStatTooLongWaiting(long val);

}
