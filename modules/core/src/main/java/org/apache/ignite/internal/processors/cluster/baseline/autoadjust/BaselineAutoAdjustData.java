/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

/**
 * Container of required data for changing baseline.
 */
class BaselineAutoAdjustData {
    /** Task represented NULL value is using when normal task can not be created. */
    public static final BaselineAutoAdjustData NULL_BASELINE_DATA = nullValue();
    /** Topology version nodes of which should be set to baseline by this task. */
    private final long targetTopologyVersion;

    /** {@code true} If this data don't actual anymore and it setting should be skipped. */
    private volatile boolean invalidated = false;
    /** {@code true} If this data was adjusted. */
    private volatile boolean adjusted = false;

    /**
     * @param targetTopologyVersion Topology version nodes of which should be set by this task.
     */
    BaselineAutoAdjustData(long targetTopologyVersion) {
        this.targetTopologyVersion = targetTopologyVersion;
    }

    /**
     * @return New null value.
     */
    private static BaselineAutoAdjustData nullValue() {
        BaselineAutoAdjustData data = new BaselineAutoAdjustData(-1);

        data.onInvalidate();
        data.onAdjust();

        return data;
    }

    /**
     * Mark that this data are invalid.
     */
    private void onInvalidate() {
        invalidated = true;
    }

    /**
     * Mark that this data was adjusted.
     */
    public void onAdjust() {
        adjusted = true;
    }

    /**
     * @return Topology version nodes of which should be set to baseline by this task.
     */
    public long getTargetTopologyVersion() {
        return targetTopologyVersion;
    }

    /**
     * @return {@code true} If this data already invalidated and can not be set.
     */
    public boolean isInvalidated() {
        return invalidated;
    }

    /**
     * @return {@code true} If this data already adjusted.
     */
    public boolean isAdjusted() {
        return adjusted;
    }

    /**
     * Produce next set baseline data based on this data.
     *
     * @return New set baseline data.
     */
    public BaselineAutoAdjustData next(long targetTopologyVersion) {
        onInvalidate();

        return new BaselineAutoAdjustData(targetTopologyVersion);
    }
}
