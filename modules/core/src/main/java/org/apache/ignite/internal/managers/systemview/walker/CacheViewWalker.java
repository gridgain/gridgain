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

package org.apache.ignite.internal.managers.systemview.walker;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link CacheView} attributes walker.
 * 
 * @see CacheView
 */
public class CacheViewWalker implements SystemViewRowAttributeWalker<CacheView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "cacheName", String.class);
        v.accept(1, "cacheId", int.class);
        v.accept(2, "cacheType", CacheType.class);
        v.accept(3, "cacheMode", CacheMode.class);
        v.accept(4, "atomicityMode", CacheAtomicityMode.class);
        v.accept(5, "cacheGroupName", String.class);
        v.accept(6, "affinity", String.class);
        v.accept(7, "affinityMapper", String.class);
        v.accept(8, "backups", int.class);
        v.accept(9, "cacheGroupId", int.class);
        v.accept(10, "cacheLoaderFactory", String.class);
        v.accept(11, "cacheStoreFactory", String.class);
        v.accept(12, "cacheWriterFactory", String.class);
        v.accept(13, "dataRegionName", String.class);
        v.accept(14, "defaultLockTimeout", long.class);
        v.accept(15, "evictionFilter", String.class);
        v.accept(16, "evictionPolicyFactory", String.class);
        v.accept(17, "expiryPolicyFactory", String.class);
        v.accept(18, "interceptor", String.class);
        v.accept(19, "isCopyOnRead", boolean.class);
        v.accept(20, "isEagerTtl", boolean.class);
        v.accept(21, "isEncryptionEnabled", boolean.class);
        v.accept(22, "isEventsDisabled", boolean.class);
        v.accept(23, "isInvalidate", boolean.class);
        v.accept(24, "isLoadPreviousValue", boolean.class);
        v.accept(25, "isManagementEnabled", boolean.class);
        v.accept(26, "isNearCacheEnabled", boolean.class);
        v.accept(27, "isOnheapCacheEnabled", boolean.class);
        v.accept(28, "isReadFromBackup", boolean.class);
        v.accept(29, "isReadThrough", boolean.class);
        v.accept(30, "isSqlEscapeAll", boolean.class);
        v.accept(31, "isSqlOnheapCacheEnabled", boolean.class);
        v.accept(32, "isStatisticsEnabled", boolean.class);
        v.accept(33, "isStoreKeepBinary", boolean.class);
        v.accept(34, "isWriteBehindEnabled", boolean.class);
        v.accept(35, "isWriteThrough", boolean.class);
        v.accept(36, "maxConcurrentAsyncOperations", int.class);
        v.accept(37, "maxQueryIteratorsCount", int.class);
        v.accept(38, "nearCacheEvictionPolicyFactory", String.class);
        v.accept(39, "nearCacheStartSize", int.class);
        v.accept(40, "nodeFilter", String.class);
        v.accept(41, "partitionLossPolicy", PartitionLossPolicy.class);
        v.accept(42, "queryDetailMetricsSize", int.class);
        v.accept(43, "queryParallelism", int.class);
        v.accept(44, "rebalanceBatchSize", int.class);
        v.accept(45, "rebalanceBatchesPrefetchCount", long.class);
        v.accept(46, "rebalanceDelay", long.class);
        v.accept(47, "rebalanceMode", CacheRebalanceMode.class);
        v.accept(48, "rebalanceOrder", int.class);
        v.accept(49, "rebalanceThrottle", long.class);
        v.accept(50, "rebalanceTimeout", long.class);
        v.accept(51, "sqlIndexMaxInlineSize", int.class);
        v.accept(52, "sqlOnheapCacheMaxSize", int.class);
        v.accept(53, "sqlSchema", String.class);
        v.accept(54, "topologyValidator", String.class);
        v.accept(55, "writeBehindBatchSize", int.class);
        v.accept(56, "writeBehindCoalescing", boolean.class);
        v.accept(57, "writeBehindFlushFrequency", long.class);
        v.accept(58, "writeBehindFlushSize", int.class);
        v.accept(59, "writeBehindFlushThreadCount", int.class);
        v.accept(60, "writeSynchronizationMode", CacheWriteSynchronizationMode.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(CacheView row, AttributeWithValueVisitor v) {
        v.accept(0, "cacheName", String.class, row.cacheName());
        v.acceptInt(1, "cacheId", row.cacheId());
        v.accept(2, "cacheType", CacheType.class, row.cacheType());
        v.accept(3, "cacheMode", CacheMode.class, row.cacheMode());
        v.accept(4, "atomicityMode", CacheAtomicityMode.class, row.atomicityMode());
        v.accept(5, "cacheGroupName", String.class, row.cacheGroupName());
        v.accept(6, "affinity", String.class, row.affinity());
        v.accept(7, "affinityMapper", String.class, row.affinityMapper());
        v.acceptInt(8, "backups", row.backups());
        v.acceptInt(9, "cacheGroupId", row.cacheGroupId());
        v.accept(10, "cacheLoaderFactory", String.class, row.cacheLoaderFactory());
        v.accept(11, "cacheStoreFactory", String.class, row.cacheStoreFactory());
        v.accept(12, "cacheWriterFactory", String.class, row.cacheWriterFactory());
        v.accept(13, "dataRegionName", String.class, row.dataRegionName());
        v.acceptLong(14, "defaultLockTimeout", row.defaultLockTimeout());
        v.accept(15, "evictionFilter", String.class, row.evictionFilter());
        v.accept(16, "evictionPolicyFactory", String.class, row.evictionPolicyFactory());
        v.accept(17, "expiryPolicyFactory", String.class, row.expiryPolicyFactory());
        v.accept(18, "interceptor", String.class, row.interceptor());
        v.acceptBoolean(19, "isCopyOnRead", row.isCopyOnRead());
        v.acceptBoolean(20, "isEagerTtl", row.isEagerTtl());
        v.acceptBoolean(21, "isEncryptionEnabled", row.isEncryptionEnabled());
        v.acceptBoolean(22, "isEventsDisabled", row.isEventsDisabled());
        v.acceptBoolean(23, "isInvalidate", row.isInvalidate());
        v.acceptBoolean(24, "isLoadPreviousValue", row.isLoadPreviousValue());
        v.acceptBoolean(25, "isManagementEnabled", row.isManagementEnabled());
        v.acceptBoolean(26, "isNearCacheEnabled", row.isNearCacheEnabled());
        v.acceptBoolean(27, "isOnheapCacheEnabled", row.isOnheapCacheEnabled());
        v.acceptBoolean(28, "isReadFromBackup", row.isReadFromBackup());
        v.acceptBoolean(29, "isReadThrough", row.isReadThrough());
        v.acceptBoolean(30, "isSqlEscapeAll", row.isSqlEscapeAll());
        v.acceptBoolean(31, "isSqlOnheapCacheEnabled", row.isSqlOnheapCacheEnabled());
        v.acceptBoolean(32, "isStatisticsEnabled", row.isStatisticsEnabled());
        v.acceptBoolean(33, "isStoreKeepBinary", row.isStoreKeepBinary());
        v.acceptBoolean(34, "isWriteBehindEnabled", row.isWriteBehindEnabled());
        v.acceptBoolean(35, "isWriteThrough", row.isWriteThrough());
        v.acceptInt(36, "maxConcurrentAsyncOperations", row.maxConcurrentAsyncOperations());
        v.acceptInt(37, "maxQueryIteratorsCount", row.maxQueryIteratorsCount());
        v.accept(38, "nearCacheEvictionPolicyFactory", String.class, row.nearCacheEvictionPolicyFactory());
        v.acceptInt(39, "nearCacheStartSize", row.nearCacheStartSize());
        v.accept(40, "nodeFilter", String.class, row.nodeFilter());
        v.accept(41, "partitionLossPolicy", PartitionLossPolicy.class, row.partitionLossPolicy());
        v.acceptInt(42, "queryDetailMetricsSize", row.queryDetailMetricsSize());
        v.acceptInt(43, "queryParallelism", row.queryParallelism());
        v.acceptInt(44, "rebalanceBatchSize", row.rebalanceBatchSize());
        v.acceptLong(45, "rebalanceBatchesPrefetchCount", row.rebalanceBatchesPrefetchCount());
        v.acceptLong(46, "rebalanceDelay", row.rebalanceDelay());
        v.accept(47, "rebalanceMode", CacheRebalanceMode.class, row.rebalanceMode());
        v.acceptInt(48, "rebalanceOrder", row.rebalanceOrder());
        v.acceptLong(49, "rebalanceThrottle", row.rebalanceThrottle());
        v.acceptLong(50, "rebalanceTimeout", row.rebalanceTimeout());
        v.acceptInt(51, "sqlIndexMaxInlineSize", row.sqlIndexMaxInlineSize());
        v.acceptInt(52, "sqlOnheapCacheMaxSize", row.sqlOnheapCacheMaxSize());
        v.accept(53, "sqlSchema", String.class, row.sqlSchema());
        v.accept(54, "topologyValidator", String.class, row.topologyValidator());
        v.acceptInt(55, "writeBehindBatchSize", row.writeBehindBatchSize());
        v.acceptBoolean(56, "writeBehindCoalescing", row.writeBehindCoalescing());
        v.acceptLong(57, "writeBehindFlushFrequency", row.writeBehindFlushFrequency());
        v.acceptInt(58, "writeBehindFlushSize", row.writeBehindFlushSize());
        v.acceptInt(59, "writeBehindFlushThreadCount", row.writeBehindFlushThreadCount());
        v.accept(60, "writeSynchronizationMode", CacheWriteSynchronizationMode.class, row.writeSynchronizationMode());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 61;
    }
}
