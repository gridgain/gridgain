package org.apache.ignite.internal.processors.query.h2.opt.statistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.opt.statistics.messages.StatsColumnData;
import org.apache.ignite.internal.processors.query.h2.opt.statistics.messages.StatsObjectData;
import org.apache.ignite.internal.processors.query.h2.opt.statistics.messages.StatsPropagationMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.gridgain.internal.h2.value.Value;

public class StatisticsUtils {
    public static StatsColumnData toMessage(ColumnStatistics stat) throws IgniteCheckedException {
        GridH2ValueMessage msgMin = stat.min() == null ? null : GridH2ValueMessageFactory.toMessage(stat.min());
        GridH2ValueMessage msgMax = stat.max() == null ? null : GridH2ValueMessageFactory.toMessage(stat.max());

        return new StatsColumnData(msgMin, msgMax, stat.nulls(), stat.cardinality(), stat.total(), stat.size(), stat.raw());
    }

    public static ColumnStatistics toColumnStatistics(StatsColumnData data) throws IgniteCheckedException {
        Value min = data.min().value(null);
        Value max = data.max().value(null);

        return new ColumnStatistics(min, max, data.nulls(), data.cardinality(), data.total(), data.size(), data.rawData());
    }



    public static StatsPropagationMessage toMessage(long reqId, String schemaName, String objectName, StatsType type,
                                                    ObjectStatistics stat) throws IgniteCheckedException {
        // TODO: pass partId and updateCounter if needed
        Map<String, StatsColumnData> columnData = new HashMap<>(stat.getColNameToStat().size());

        for(Map.Entry<String, ColumnStatistics> ts : stat.getColNameToStat().entrySet())
            columnData.put(ts.getKey(), toMessage(ts.getValue()));

        StatsObjectData data;
        if (stat instanceof ObjectPartitionStatistics) {
            ObjectPartitionStatistics partStats = (ObjectPartitionStatistics) stat;
            data = new StatsObjectData(schemaName, objectName, stat.rowCount(), type, partStats.partId(),
                    partStats.updCnt(), columnData);
        } else
            data = new StatsObjectData(schemaName, objectName, stat.rowCount(), type, 0,0, columnData);

        return new StatsPropagationMessage(reqId, Collections.singletonList(data));
    }

    public static ObjectPartitionStatistics toObjectPartitionStatistics(StatsPropagationMessage data) throws IgniteCheckedException {
        if (data == null)
            return null;

        assert data.data().size() == 1;

        StatsObjectData objData = data.data().get(0);

        assert objData.type == StatsType.PARTITION;

        Map<String, ColumnStatistics> colNameToStat = new HashMap<>(objData.data.size());

        for (Map.Entry<String, StatsColumnData> cs : objData.data.entrySet()) {
            colNameToStat.put(cs.getKey(), toColumnStatistics(cs.getValue()));
        }

        return new ObjectPartitionStatistics(objData.partId, true, objData.rowsCnt, objData.updCnt, colNameToStat);
    }

    public static ObjectStatistics toObjectStatistics(StatsPropagationMessage data) throws IgniteCheckedException {
        if (data == null)
            return null;

        assert data.data().size() == 1;

        StatsObjectData objData = data.data().get(0);
        Map<String, ColumnStatistics> colNameToStat = new HashMap<>(objData.data.size());

        for (Map.Entry<String, StatsColumnData> cs : objData.data.entrySet()) {
            colNameToStat.put(cs.getKey(), toColumnStatistics(cs.getValue()));
        }

        return new ObjectStatistics(objData.rowsCnt, colNameToStat);
    }

}
