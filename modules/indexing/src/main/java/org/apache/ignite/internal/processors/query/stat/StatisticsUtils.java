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
package org.apache.ignite.internal.processors.query.stat;

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.processors.query.stat.messages.StatsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatsObjectData;
import org.gridgain.internal.h2.value.Value;

/**
 * Utilities to convert statistics from/to messages.
 */
public class StatisticsUtils {
    /**
     * Convert ColumnStatistics to StaticColumnData message.
     *
     * @param stat Column statistics to convert.
     * @return Converted stats column data message.
     * @throws IgniteCheckedException In case of errors.
     */
    public static StatsColumnData toMessage(ColumnStatistics stat) throws IgniteCheckedException {
        GridH2ValueMessage msgMin = stat.min() == null ? null : GridH2ValueMessageFactory.toMessage(stat.min());
        GridH2ValueMessage msgMax = stat.max() == null ? null : GridH2ValueMessageFactory.toMessage(stat.max());

        return new StatsColumnData(msgMin, msgMax, stat.nulls(), stat.cardinality(), stat.total(), stat.size(), stat.raw());
    }

    /**
     * Convert statistics column data message to column statistics object.
     *
     * @param ctx Kernal context.
     * @param data Statistics column data message to convert.
     * @return ColumnStatistics object.
     * @throws IgniteCheckedException In case of errors.
     */
    public static ColumnStatistics toColumnStatistics(GridKernalContext ctx, StatsColumnData data) throws IgniteCheckedException {
        Value min = (data.min() == null) ? null : data.min().value(ctx);
        Value max = (data.max() == null) ? null : data.max().value(ctx);

        return new ColumnStatistics(min, max, data.nulls(), data.cardinality(), data.total(), data.size(), data.rawData());
    }

    /**
     * Build statistics object data from values.
     *
     * @param key Statistics key.
     * @param type Statistics type.
     * @param stat Object statistics to convert.
     * @return Converted StatsObjectData message.
     * @throws IgniteCheckedException In case of errors.
     */
    public static StatsObjectData toMessage(StatsKey key, StatsType type, ObjectStatisticsImpl stat)
            throws IgniteCheckedException {
        Map<String, StatsColumnData> colData = new HashMap<>(stat.columnsStatistics().size());

        for (Map.Entry<String, ColumnStatistics> ts : stat.columnsStatistics().entrySet())
            colData.put(ts.getKey(), toMessage(ts.getValue()));

        StatsObjectData data;
        if (stat instanceof ObjectPartitionStatisticsImpl) {
            ObjectPartitionStatisticsImpl partStats = (ObjectPartitionStatisticsImpl) stat;
            data = new StatsObjectData(key, stat.rowCount(), type, partStats.partId(),
                    partStats.updCnt(), colData);
        }
        else
            data = new StatsObjectData(key, stat.rowCount(), type, 0,0, colData);
        return data;
    }

    /**
     * Convert StatsObjectData message to ObjectPartitionStatistics.
     *
     * @param ctx Kernal context to use during convertation.
     * @param objData StatsObjectData to convert.
     * @return Converted ObjectPartitionStatistics.
     * @throws IgniteCheckedException In case of errors.
     */
    public static ObjectPartitionStatisticsImpl toObjectPartitionStatistics(
            GridKernalContext ctx,
            StatsObjectData objData
    ) throws IgniteCheckedException {
        if (objData == null)
            return null;

        assert objData.type() == StatsType.PARTITION;

        Map<String, ColumnStatistics> colNameToStat = new HashMap<>(objData.data().size());

        for (Map.Entry<String, StatsColumnData> cs : objData.data().entrySet())
            colNameToStat.put(cs.getKey(), toColumnStatistics(ctx, cs.getValue()));

        return new ObjectPartitionStatisticsImpl(objData.partId(), true, objData.rowsCnt(), objData.updCnt(),
                colNameToStat);
    }
}
