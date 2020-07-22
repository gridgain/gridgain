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
package org.apache.ignite.internal.visor.statistics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 *
 */
public class MessageStatsTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Map<String, Long> monotonicMetric;

    /** */
    private long[] bounds;

    /** */
    private Map<String, long[]> histograms;

    /** */
    public MessageStatsTaskResult() {

    }

    /** */
    public MessageStatsTaskResult(Map<String, Long> monotonicMetric, long[] bounds, Map<String, long[]> histograms) {
        assert monotonicMetric != null;
        assert bounds != null;
        assert histograms != null;
        assert monotonicMetric.size() == histograms.size();

        histograms.values().forEach(v -> { assert v.length == bounds.length + 1; });
        histograms.keySet().forEach(k -> { assert monotonicMetric.keySet().contains(k); });

        this.monotonicMetric = monotonicMetric;
        this.bounds = bounds;
        this.histograms = histograms;
    }

    /** */
    public Map <String, Long> monotonicMetric() {
        return monotonicMetric;
    }

    /** */
    public long[] bounds() {
        return bounds;
    }

    /** */
    public Map<String, long[]> histograms() {
        return histograms;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, monotonicMetric);

        U.writeLongArray(out, bounds);

        U.writeMap(out, histograms);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        monotonicMetric = U.readMap(in);

        bounds = U.readLongArray(in);

        histograms = U.readMap(in);
    }
}
