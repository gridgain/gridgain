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

package org.apache.ignite.internal.processors.query.h2.statistics;

import org.h2.value.Value;

public class ColumnStatistics {
    private final Value min;

    private final Value max;

    private final int nulls;

    private final int selectivity;

    public ColumnStatistics(Value min, Value max, long totalRowCnt, long nullsCnt, long distinctValuesCnt) {
        this.min = min;
        this.max = max;

        if (totalRowCnt > 0)
            nulls = (int)(100 * nullsCnt / totalRowCnt);
        else
            nulls = 0;

        if (totalRowCnt - nullsCnt > 0)
            selectivity = (int)(100 * distinctValuesCnt / (totalRowCnt - nullsCnt));
        else
            selectivity = 0;
    }

    public Value min() {
        return min;
    }

    public Value max() {
        return max;
    }

    public int nulls() {
        return nulls;
    }

    public int selectivity() {
        return selectivity;
    }
}
