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
package org.apache.ignite.internal.processors.metrics.impl;

import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toJson;

/**
 *
 */
public class MetricUtilsTest {
    /** */
    @Test
    public void testToJson() {
        HistogramMetric singleBucket = new HistogramMetric("singleBucket", "", new long[] { 1 });

        singleBucket.value(1);

        String singleBucketJson = toJson(singleBucket);

        System.out.println(singleBucketJson);

        singleBucketJson.equals("{\"bounds\":[1],\"values\":" +
            "[{\"fromExclusive\":0,\"toInclusive\":1,\"value\":1},{\"fromExclusive\":1,\"value\":0}]}"
        );

        HistogramMetric histo = new HistogramMetric("histo", "", new long[] { 1, 5, 10, 20, 50 });

        histo.value(1);
        histo.value(12);
        histo.value(13);
        histo.value(35);
        histo.value(100);

        String histoJson = toJson(histo);

        System.out.println(histoJson);

        histoJson.equals("{\"bounds\":[1,5,10,20,50],\"values\":" +
            "[{\"fromExclusive\":0,\"toInclusive\":1,\"value\":1}, " +
            "{\"fromExclusive\":1,\"toInclusive\":5,\"value\":0}, " +
            "{\"fromExclusive\":5,\"toInclusive\":10,\"value\":0}, " +
            "{\"fromExclusive\":10,\"toInclusive\":20,\"value\":2}, " +
            "{\"fromExclusive\":20,\"toInclusive\":50,\"value\":1}, " +
            "{\"fromExclusive\":50,\"value\":1}]}"
        );
    }
}
