/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat.config;

import java.io.Serializable;
import java.util.Objects;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class StatisticsCollectConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final double DFLT_SAMPLES = 1.0;

    /** Samples */
    private double samples = DFLT_SAMPLES;

    /** */
    public double samples() {
        return samples;
    }

    /** */
    public void samples(double samples) {
        assert samples >=0 && samples <=1 : "Invalid samples: must be inside [0, 1] interval";

        this.samples = samples;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        StatisticsCollectConfiguration that = (StatisticsCollectConfiguration)o;

        if (Double.compare(samples, that.samples) != 0)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(samples);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsCollectConfiguration.class, this);
    }
}
