/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.systemview.view;

import java.util.Date;
import org.apache.ignite.internal.managers.systemview.walker.Order;

/**
 * Pages timestamp histogramm representation for a {@link SystemView}.
 */
public class PagesTimestampHistogramView {
    /** Data region name. */
    private final String dataRegionName;

    /** Start of timestamps interval. */
    private final long intervalStart;

    /** End of timestamps interval. */
    private final long intervalEnd;

    /** Count of pages last accessed within given interval. */
    private final long pagesCnt;

    /**
     * @param dataRegionName Data region name.
     * @param intervalStart Start of timestamps interval.
     * @param intervalEnd End of timestamps interval.
     * @param pagesCnt Count of pages last accessed within given interval.
     */
    public PagesTimestampHistogramView(String dataRegionName, long intervalStart, long intervalEnd, long pagesCnt) {
        this.dataRegionName = dataRegionName;
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.pagesCnt = pagesCnt;
    }

    /** @return Data region name. */
    @Order
    public String dataRegionName() {
        return dataRegionName;
    }

    /** @return Start of timestamps interval. */
    @Order(1)
    public Date intervalStart() {
        return new Date(intervalStart);
    }

    /** @return End of timestamps interval. */
    @Order(2)
    public Date intervalEnd() {
        return new Date(intervalEnd);
    }

    /** @return Count of pages last accessed within given interval. */
    @Order(3)
    public long pagesCount() {
        return pagesCnt;
    }
}
