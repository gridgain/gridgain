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
package org.apache.ignite.internal.commandline.walconverter;

/**
 * Statistic for record size, used to accumulate information about several record and calculating average.
 */
public class RecordSizeCountStat {
    /** Sum of sizes. */
    private long size = -1;

    /** Count of all records. */
    private int cnt;

    /**
     * @param size record size. Negative value means size is unknown for current occurrence. Any negative value resets
     * accumulated statistics.
     */
    void occurrence(int size) {
        if (size >= 0) {
            if (this.size < 0)
                this.size = 0;

            this.size += size;
        }
        else {
            if (this.size > 0)
                this.size = -1;
        }

        cnt++;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return (size >= 0 ? size : "") +
            "\t " + cnt +
            "\t " + ((size >= 0) ? size / cnt : "");
    }

    /**
     * @return average size of one record, as double.
     */
    private double averageD() {
        return 1.0 * size / cnt;
    }

    /**
     * @return average size of one record, printable version.
     */
    String averageStr() {
        return cnt == 0 ? "" : String.format("%.2f", averageD());
    }

    /**
     * @return Count of all records.
     */
    int getCount() {
        return cnt;
    }

    /**
     * @param val other statistic value to reduce with.
     */
    void add(RecordSizeCountStat val) {
        cnt += val.cnt;
        size += val.size;
    }
}
