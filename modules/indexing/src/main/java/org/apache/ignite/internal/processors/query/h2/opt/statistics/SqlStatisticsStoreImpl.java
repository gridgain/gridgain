package org.apache.ignite.internal.processors.query.h2.opt.statistics;


import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.opt.statistics.messages.StatsPropagationMessage;
import org.apache.ignite.resources.LoggerResource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Sql statistics storage in metastore
 */
public class SqlStatisticsStoreImpl implements SqlStatisticsStore, MetastorageLifecycleListener {

    // In local meta store it store 3 type of keys:
    // 1) partitions statistics - data_stats_part.<SCHEMA>.<OBJECT>.<partId>
    // 2) local statistics - data_stats_local.<SCHEMA>.<OBJECT>
    // 3) global statistics - data_stats_glob.<SCHEMA>.<OBJECT>
    private final static String META_SEPARATOR = ".";
    private final static String META_STAT_PREFIX = "data_stats";
    private final static String META_PART_STAT_PREFIX = META_STAT_PREFIX + "_part";
    private final static String META_LOCAL_STAT_PREFIX = META_STAT_PREFIX + "_local";
    private final static String META_GLOB_STAT_PREFIX = META_STAT_PREFIX + "_glob";

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    private final GridKernalContext ctx;
    private SqlStatisticsRepository repository;
    private ReadWriteMetastorage metastore;

    /**
     * Constructor.
     *
     * @param ctx grid kernal context.
     */
    public SqlStatisticsStoreImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Start with specified statistics repository.
     *
     * @param repository repository to fullfill on metastore available.
     */
    public void start(SqlStatisticsRepository repository) {
        this.repository = repository;
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    private void writePartStatistics(String schemaName, String tblName, ObjectPartitionStatistics partStats) {
        if (metastore == null) {
            log.warning(String.format("Metastore not ready to save partition statistic: %s.%s part %d", schemaName,
                    tblName, partStats.partId()));
            return;
        }

        //TODO: writeMeta();
    }

    //rivate Serializable prepareStat()

    private void writeTblStatistics(String schemaName, String tblName, ObjectStatistics tblStats) {
        if (metastore == null) {
            log.warning(String.format("Metastore not ready to save table statistic: %s.%s", schemaName));
            return;
        }
    }

    private String getTblName(String metaKey) {
        int idx = metaKey.lastIndexOf(META_SEPARATOR) + 1;

        assert idx < metaKey.length();

        return metaKey.substring(idx);
    }

    private String getSchemaName(String metaKey) {
        int schemaIdx = metaKey.indexOf(META_SEPARATOR) + 1;
        int tableIdx = metaKey.indexOf(META_SEPARATOR, schemaIdx + 1);

        return metaKey.substring(schemaIdx, tableIdx);
    }

    private int getPartitionId(String metaKey) {
        int partIdx = metaKey.lastIndexOf(META_SEPARATOR);
        String partIdStr = metaKey.substring(partIdx);
        return Integer.valueOf(partIdStr);
    }

    private QueryTable getQueryTable(String metaKey) {
        int schemaIdx = metaKey.indexOf(META_SEPARATOR) + 1;
        int tableIdx = metaKey.indexOf(META_SEPARATOR, schemaIdx + 1);

        return new QueryTable(metaKey.substring(schemaIdx, tableIdx), metaKey.substring(tableIdx + 1));
    }

    private String getGlobalKey(String schema, String tblName) {
        return META_GLOB_STAT_PREFIX + META_SEPARATOR + schema + META_SEPARATOR + tblName;
    }

    private String getLocalKey(String schema, String tblName) {
        return META_LOCAL_STAT_PREFIX + META_SEPARATOR + schema + META_SEPARATOR + tblName;
    }


    private String getPartKeyPrefix(String schema, String tblName) {
        return META_PART_STAT_PREFIX + META_SEPARATOR + schema + META_SEPARATOR + tblName + META_SEPARATOR;
    }

    @Override
    public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {

        metastorage.iterate(META_LOCAL_STAT_PREFIX, (key, statMsg) -> {
            System.out.println(key + statMsg);
            QueryTable tbl = getQueryTable(key);
            try {
                ObjectStatistics statistics = StatisticsUtils.toObjectStatistics((StatsPropagationMessage)statMsg);

                repository.cacheLocalStatistics(tbl, statistics);
            } catch (IgniteCheckedException e) {
                // TODO:
            }
            if (log.isDebugEnabled()) {
                log.debug("Local statistics for table " + tbl + " loaded");
            }
        },true);


    }

    @Override
    public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastore = metastorage;
    }

    private void writeMeta(String key, Serializable object) throws IgniteCheckedException {
        assert object != null;

        if (metastore == null)
            // TODO log warn
            return;

        ctx.cache().context().database().checkpointReadLock();
        try {
            metastore.write(key, object);
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    private Serializable readMeta(String key) throws IgniteCheckedException {
        ctx.cache().context().database().checkpointReadLock();
        try {
            return metastore.read(key);
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    private void removeMeta(String key) throws IgniteCheckedException {
        ctx.cache().context().database().checkpointReadLock();
        try {
            metastore.remove(key);
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    private void removeMeta(Collection<String> keys) throws IgniteCheckedException {
        ctx.cache().context().database().checkpointReadLock();
        try {
            for (String key : keys)
                metastore.remove(key);
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    private void iterateMeta(String keyPrefix, BiConsumer<String, ? super Serializable> cb, boolean unmarshall) throws IgniteCheckedException {
        assert metastore != null;

        ctx.cache().context().database().checkpointReadLock();
        try {
            metastore.iterate(keyPrefix, cb, unmarshall);
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    @Override
    public void clearAllStatistics() {
        if (metastore == null)
            return;

        try {
            iterateMeta(META_STAT_PREFIX, (k,v) -> {
                try {
                    metastore.remove(k);
                } catch (IgniteCheckedException e) {
                    // TODO
                }
            }, false);
        } catch (IgniteCheckedException e) {
            // TODO
        }
    }

    @Override
    public void saveLocalPartitionsStatistics(QueryTable tbl, Collection<ObjectPartitionStatistics> statistics) {
        if (metastore == null)
            // TODO: log warning
            return;
        Map<Integer, ObjectPartitionStatistics> partitionStatistics = statistics.stream().collect(
                Collectors.toMap(ObjectPartitionStatistics::partId, s -> s));

        try {
            iterateMeta(getPartKeyPrefix(tbl.schema(), tbl.table()), (k,v) -> {
                ObjectPartitionStatistics newStats = partitionStatistics.get(getPartitionId(k));
                try {
                    if (newStats == null)
                        metastore.remove(k);
                    else
                        metastore.write(k, StatisticsUtils.toMessage(-1L, tbl.schema(), tbl.table(), StatsType.PARTITION, newStats));

                } catch (IgniteCheckedException e) {
                    // TODO
                }
            }, false);
        } catch (IgniteCheckedException e) {
            // TODO
        }
    }

    @Override
    public Collection<ObjectPartitionStatistics> getLocalPartitionsStatistics(QueryTable tbl) {
        if (metastore == null)
            return null;
        List<ObjectPartitionStatistics> result = new ArrayList<>();
        try {
            iterateMeta(getPartKeyPrefix(tbl.schema(), tbl.table()), (k,v) -> {
                try {
                    ObjectPartitionStatistics partStats = StatisticsUtils.toObjectPartitionStatistics((StatsPropagationMessage)v);
                    result.add(partStats);
                } catch (IgniteCheckedException e) {
                    // TODO
                }
            }, true);
        } catch (IgniteCheckedException e) {
            // TODO
        }
        return result;
    }

    @Override
    public void clearLocalPartitionsStatistics(QueryTable tbl) {
        if (metastore == null)
            return;

        try {
            iterateMeta(getPartKeyPrefix(tbl.schema(), tbl.table()), (k,v) -> {
                try {
                    metastore.remove(k);
                } catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }
            }, false);
        } catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveLocalPartitionStatistics(QueryTable tbl, int partId, ObjectPartitionStatistics statistics) {
        if (metastore == null)
            return;
        String partPrefix = getPartKeyPrefix(tbl.schema(), tbl.table()) + partId;
        StatsPropagationMessage statsMessage = null;
        try {
            statsMessage = StatisticsUtils.toMessage(-1L, tbl.schema(), tbl.table(), StatsType.PARTITION, statistics);
            writeMeta(partPrefix, statsMessage);
        } catch (IgniteCheckedException e) {
            // TODO
        }

    }

    @Override
    public ObjectPartitionStatistics getLocalPartitionStatistics(QueryTable tbl, int partId) {
        if (metastore == null)
            return null;
        String metaKey = getPartKeyPrefix(tbl.schema(), tbl.table()) + partId;
        try {
            return StatisticsUtils.toObjectPartitionStatistics((StatsPropagationMessage) readMeta(metaKey));
        } catch (IgniteCheckedException e) {
            // TODO
        }
        return null;
    }

    @Override
    public void clearLocalPartitionStatistics(QueryTable tbl, int partId) {
        if (metastore == null)
            return;
        String metaKey = getPartKeyPrefix(tbl.schema(), tbl.table()) + partId;
        try {
            removeMeta(metaKey);
        } catch (IgniteCheckedException e) {
            // TODO
        }
    }

    @Override
    public void saveLocalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        String metaKey = getLocalKey(tbl.schema(), tbl.table());
        try {
            StatsPropagationMessage statsMessage = StatisticsUtils.toMessage(-1, tbl.schema(), tbl.table(),
                    StatsType.LOCAL, statistics);

            writeMeta(metaKey, statsMessage);
        } catch (IgniteCheckedException e) {
            // TODO
        }

    }

    @Override
    public ObjectStatistics getLocalStatistics(QueryTable tbl, boolean tryLoad) {
        if (metastore == null)
            return null;
        String metaKey = getLocalKey(tbl.schema(), tbl.table());
        try {
            return StatisticsUtils.toObjectStatistics((StatsPropagationMessage)readMeta(metaKey));
        } catch (IgniteCheckedException e) {
            // TODO
        }
        return null;
    }

    @Override
    public void clearLocalStatistics(QueryTable tbl) {
        String metaKey = getLocalKey(tbl.schema(), tbl.table());
        try {
            removeMeta(metaKey);
        } catch (IgniteCheckedException e) {
            // TODO
        }
    }

    @Override
    public void saveGlobalStatistics(QueryTable tbl, ObjectStatistics statistics) {
        String metaKey = getGlobalKey(tbl.schema(), tbl.table());

        StatsPropagationMessage statsMessage = null;
        try {
            statsMessage = StatisticsUtils.toMessage(-1, tbl.schema(), tbl.table(),
                    StatsType.GLOBAL, statistics);

            writeMeta(metaKey, statsMessage);
        } catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public ObjectStatistics getGlobalStatistics(QueryTable tbl, boolean tryLoad) {
        if (metastore == null)
            return null;
        String metaKey = getGlobalKey(tbl.schema(), tbl.table());
        try {
            return StatisticsUtils.toObjectStatistics((StatsPropagationMessage)readMeta(metaKey));
        } catch (IgniteCheckedException e) {
            // TODO
        }
        return null;
    }

    @Override
    public void clearGlobalStatistics(QueryTable tbl) {
        String metaKey = getGlobalKey(tbl.schema(), tbl.table());

        try {
            removeMeta(metaKey);
        } catch (IgniteCheckedException e) {
            // TODO
        }
    }
}
