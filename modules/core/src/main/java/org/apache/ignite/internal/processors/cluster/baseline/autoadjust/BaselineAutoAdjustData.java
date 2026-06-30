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

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import org.apache.ignite.AutoAdjustMode;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.AutoAdjustMode.SCALE_DOWN;
import static org.apache.ignite.AutoAdjustMode.SCALE_UP;

/**
 * Container of required data for changing baseline.
 */
class BaselineAutoAdjustData {
    /** Task represented NULL value is using when normal task can not be created. */
    public static final BaselineAutoAdjustData NULL_BASELINE_DATA = nullValue();

    /** Topology version nodes of which should be set to baseline by this task. */
    private final long targetTopologyVersion;

    /** {@code true} If this data isn't actual anymore and this setting should be skipped. */
    private volatile boolean invalidated;

    /** {@code true} If this data has been adjusted. */
    private volatile boolean adjusted;

    /** {@code true} If this data has been adjusted for scale up. */
    private volatile boolean scaleUpAdjusted;

    /** {@code true} If this data has been adjusted for scale down. */
    private volatile boolean scaleDownAdjusted;

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
        data.onAdjust(SCALE_UP);
        data.onAdjust(SCALE_DOWN);

        return data;
    }

    /**
     * Mark that this data are invalid.
     */
    private void onInvalidate() {
        invalidated = true;
    }

    /**
     * Marks that this data has been adjusted.
     * @deprecated Use {@link #onAdjust(AutoAdjustMode)} instead.
     */
    @Deprecated
    public void onAdjust() {
        adjusted = true;
    }

    /**
     * Marks that data has been adjusted for the scale direction corresponding to the provided auto-adjust mode
     * {@link AutoAdjustMode}.
     *
     * @param mode The baseline scale direction.
     */
    public void onAdjust(AutoAdjustMode mode) {
        switch (mode) {
            case SCALE_UP:
                scaleUpAdjusted = true;
                break;
            case SCALE_DOWN:
                scaleDownAdjusted = true;
                break;
            case SCALE_UP_DOWN:
                adjusted = true;
        }
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
     * @deprecated Use {@link #isAdjusted(AutoAdjustMode)} instead.
     */
    @Deprecated
    public boolean isAdjusted() {
        return adjusted;
    }

    /**
     * Returns whether the data has been adjusted for the direction corresponding to the provided auto-adjust mode
     * {@link AutoAdjustMode}.
     *
     * @param mode The baseline scale direction.
     * @return {@code true} If this data already adjusted.
     */
    public boolean isAdjusted(AutoAdjustMode mode) {
        switch (mode) {
            case SCALE_UP:
                return scaleUpAdjusted;
            case SCALE_DOWN:
                return scaleDownAdjusted;
            case SCALE_UP_DOWN:
                return adjusted;
            default:
                throw new IgniteException("Unsupported auto-adjust: " + mode + ". Use SCALE_UP or SCALE_DOWN.");
        }
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

    /** {@inheritDoc */
    @Override public String toString() {
        return S.toString(BaselineAutoAdjustData.class, this);
    }
}
