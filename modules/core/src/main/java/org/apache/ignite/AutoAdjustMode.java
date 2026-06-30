/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite;

/**
 * Baseline auto-adjustment mode.
 */
public enum AutoAdjustMode {
    /**
     * It's the old option. It corresponds to the baseline scale up and down simultaneously.
     *
     * @deprecated Use {@link #SCALE_UP} or {@link #SCALE_DOWN} instead.
     */
    @Deprecated
    SCALE_UP_DOWN(""),

    /** Scale up auto adjustment. */
    SCALE_UP("scale up"),

    /** Scale down auto adjustment. */
    SCALE_DOWN("scale down");

    /** */
    private final String label;

    /**
     * @param label The auto-adjust mode's label.
     */
    AutoAdjustMode(String label) {
        this.label = label;
    }

    /**
     * @return Auto-adjust label.
     */
    public String getLabel() {
        return label;
    }
}
