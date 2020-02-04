/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.checker.tasks;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects keys with their {@link GridCacheVersion} according to a recheck list.
 */
public class RepairRequestTask extends ComputeTaskAdapter<RepairRequest, Map<PartitionKeyVersion, Map<UUID, VersionedValue>>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    private RepairRequest repairReq;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,  RepairRequest arg)
        throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        repairReq = arg;

        Map<UUID, Map<KeyCacheObject, Map<UUID, VersionedValue>>> targetNodesToData = new HashMap<>();

        for (Map.Entry<KeyCacheObject, Map<UUID, VersionedValue>> dataEntry: repairReq.data().entrySet()) {
            KeyCacheObject keyCacheObject = null;
            try {
                keyCacheObject = unmarshalKey(dataEntry.getKey(), ignite.cachex(repairReq.cacheName()).context());
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }

            Object key = keyCacheObject.value(
                ignite.cachex(repairReq.cacheName()).context().cacheObjectContext(), false);

            UUID primaryNodeId = ignite.affinity(
                repairReq.cacheName()).mapKeyToPrimaryAndBackups(key).iterator().next().id();

            targetNodesToData.putIfAbsent(primaryNodeId, new HashMap<>());

            targetNodesToData.get(primaryNodeId).put(keyCacheObject, dataEntry.getValue());
        }

        for (ClusterNode node : subgrid) {
            Map<KeyCacheObject, Map<UUID, VersionedValue>> data = targetNodesToData.remove(node.id());

            if (data != null && !data.isEmpty()) {
                // TODO: 03.12.19 Use proper top ver instead of null;
                // TODO: 03.12.19 PartitionKeyVersion is used in order to prevent finishUnmarshal problem, cause actually we only need keyCacheObject,
                // TODO: 03.12.19consider using better wrapper here.
                jobs.put(
                    new RepairJob(data.entrySet().stream().collect(
                        Collectors.toMap(
                            entry -> new PartitionKeyVersion(null, entry.getKey(), null),
                            entry -> entry.getValue())),
                        arg.cacheName()),
                    node);
            }
        }

        if (!targetNodesToData.isEmpty()) {
            // TODO: 03.12.19 Print warning that sort of affinity awareness is not possible, so that for all other data random node will be used.
            for (Map<KeyCacheObject, Map<UUID, VersionedValue>> data : targetNodesToData.values()) {
                // TODO: 03.12.19 Use random node instead.
                ClusterNode node = subgrid.iterator().next();
                // TODO: 03.12.19 Use proper top ver instead of null;
                // TODO: 03.12.19 PartitionKeyVersion is used in order to prevent finishUnmarshal problem, cause actually we only need keyCacheObject,
                // TODO: 03.12.19consider using better wrapper here.
                jobs.put(
                    new RepairJob(data.entrySet().stream().collect(
                        Collectors.toMap(
                            entry -> new PartitionKeyVersion(null, entry.getKey(), null),
                            entry -> entry.getValue())),
                        arg.cacheName()),
                    node);
            }
        }

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == ComputeJobResultPolicy.FAILOVER) {
            superRes = ComputeJobResultPolicy.WAIT;

            log.warning("CollectPartitionEntryHashesJob failed on node " +
                "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
        }

        return superRes;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKeyVersion, Map<UUID, VersionedValue>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        Map<PartitionKeyVersion, Map<UUID, VersionedValue>> res = new HashMap<>();

        for (ComputeJobResult result : results)
            res.putAll(result.getData());

        return res;
    }

    /**
     *
     */
    public static class RepairJob extends ComputeJobAdapter {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Partition key. */
        private final Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data;

        private String cacheName;

        public RepairJob(Map<PartitionKeyVersion, Map<UUID, VersionedValue>> data, String cacheName) {
            this.data = data;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        // TODO: 02.12.19
        @Override public  Map<PartitionKeyVersion, Map<UUID, VersionedValue>> execute() throws IgniteException {
            Map<PartitionKeyVersion, Map<UUID, VersionedValue>> recheckedKeys = new HashMap<>();

            for (Map.Entry<PartitionKeyVersion, Map<UUID, VersionedValue>> dataEntry: data.entrySet()) {

                Object key = null;
                try {
                    key =  unmarshalKey(
                        dataEntry.getKey().getKey(),
                        ignite.cachex(cacheName).context()).value(
                            ignite.cachex(cacheName).context().cacheObjectContext(),
                        false);
                }
                catch (IgniteCheckedException e) {
                    // TODO: 03.12.19 Use proper message here.
                    e.printStackTrace();
                }

                Map<UUID, VersionedValue> nodeToVersionedValue = dataEntry.getValue();

                GridCacheVersion maxVer = new GridCacheVersion(0,0,0);

                CacheObject maxVal = null;

                // TODO: 03.12.19 Consider using lambda instead.
                for (VersionedValue versionedValue: nodeToVersionedValue.values()) {
                    if (versionedValue.version().compareTo(maxVer) > 0) {
                        maxVer = versionedValue.version();

                        maxVal = versionedValue.value();
                    }
                }

                // TODO: 02.12.19
                int removeQueueMaxSize = 32;

                // TODO: 03.12.19 Generify with boolean.
                Boolean keyWasSuccessfullyFixed = (Boolean) ignite.cache(cacheName).invoke(
                    key,
                    // TODO: 03.12.19 Given data is excessive for atomic caches.
                    new RepairEntryProcessor(maxVal, nodeToVersionedValue, removeQueueMaxSize));

                if (!keyWasSuccessfullyFixed) {
                    recheckedKeys.put(dataEntry.getKey(), dataEntry.getValue());
                }
            }

            return recheckedKeys;
        }

        public class RepairEntryProcessor implements EntryProcessor {

            /** Value to set. */
            private Object val;

            Map<UUID, VersionedValue> data;

            private long rmvQueueMaxSize;

            /** */
            public RepairEntryProcessor(Object val,  Map<UUID, VersionedValue> data, long rmvQueueMaxSize) {
                this.val = val;
                this.data = data;
                this.rmvQueueMaxSize = rmvQueueMaxSize;
            }

            @SuppressWarnings("unchecked")
            @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
                CacheEntry verEntry = (CacheEntry)entry.unwrap(CacheEntry.class);

                Comparable currKeyGridCacheVer = verEntry.version();

                GridCacheContext cctx = (GridCacheContext)entry.unwrap(GridCacheContext.class);

                UUID localNodeId = cctx.localNodeId();

                VersionedValue versionedValue = data.get(localNodeId);

                if (entry.exists()) {
                    if (currKeyGridCacheVer.compareTo(versionedValue.version()) == 0)
                        entry.setValue(val);

                    return true;
                }
                else {
                    if (currKeyGridCacheVer.compareTo(new GridCacheVersion(0, 0, 0)) != 0) {
                        if (currKeyGridCacheVer.compareTo(versionedValue.version()) == 0)
                            entry.setValue(val);

                        return true;
                    }
                    else {
                        boolean inEntryTTLBounds =
                            (System.currentTimeMillis() - versionedValue.recheckStartTime()) <
                                Long.decode(IGNITE_CACHE_REMOVED_ENTRIES_TTL);

                        long currUpdateCntr = cctx.topology().localPartition(
                            cctx.cache().affinity().partition(entry.getKey())).updateCounter();

                        boolean inDeferredDelQueueBounds = ((currUpdateCntr - versionedValue.updateCounter()) <
                            rmvQueueMaxSize);

                        if (inEntryTTLBounds && inDeferredDelQueueBounds) {
                            entry.setValue(val);

                            return true;
                        }
                        else
                            return false;
                    }
                }
            }
        }
    }
}
