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

package org.apache.ignite.console.metrics;

import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class MetricDto {
    /** */
    private String name;

    /** */
    private String val;

    /**
     * @return Metric name.
     */
    public String getName() {
        return name;
    }

    /**
     * Default constructor for serialization.
     */
    public MetricDto() {
        // No-op.
    }

    /**
     * Constructor from raw data.
     *
     * @param data Raw data.
     */
    public MetricDto(String data) {
        String[] ss = data.split("=");

        if (F.isEmpty(ss))
            throw new IllegalStateException("Invalid data for metrics: " + data);

        name = ss[0];
        val = ss[1];
    }

    /**
     * Full constructor.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public MetricDto(String name, String val) {
        this.name = name;
        this.val = val;
    }

    /**
     * @param name Metric name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Metric value.
     */
    public String getValue() {
        return val;
    }

    /**
     * @param val Metric value.
     */
    public void setValue(String val) {
        this.val = val;
    }
}
