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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntriesExtended;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationRepairMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationValueMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;
import org.apache.ignite.internal.processors.cache.verify.SensitiveMode;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.io.File.separatorChar;
import static org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries.getConflictsAsString;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.createLocalResultFile;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.mapPartitionReconciliation;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.objectStringView;
import static org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder.SkippingReason.KEY_WAS_NOT_REPAIRED;
import static org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder.SkippingReason.LOST_PARTITION;
import static org.apache.ignite.internal.util.IgniteUtils.EMPTY_BYTES;

/**
 * Defines a contract for collecting of inconsistent and repaired entries.
 */
public interface ReconciliationResultCollector {
    /**
     * Appends skipped entries.
     *
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param keys Skipped entries
     * @throws IgniteException If cache with the given name does not exist.
     */
    void appendSkippedEntries(String cacheName, int partId, Map<VersionedKey, Map<UUID, VersionedValue>> keys) throws IgniteException;

    /**
     * Appends skipped partition.
     *
     * @param cacheName Cache name.
     * @param partId Partition id.
     */
    void appendSkippedPartition(String cacheName, int partId);

    /**
     * Appends conflicted entries.
     * When this result collector has a registered conflict for a key, the existing one is updated with a new value.
     *
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param conflicts Conflicted entries.
     * @param actualKeys Actual values.
     * @throws IgniteException If cache with the given name does not exist.
     */
    void appendConflictedEntries(
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys) throws IgniteException;

    /**
     * Returns the total number of conflicts.
     *
     * @return The total number of conflicts.
     */
    long conflictedEntriesSize();

    /**
     * Returns the total number of conflicts for the specified cache.
     *
     * @param cacheName Cache name.
     * @return The total number of conflicts for the specified cache.
     */
    long conflictedEntriesSize(String cacheName);

    /**
     * Appends repaired entries.
     *
     * @param cacheName Cache name.
     * @param partId Partition Id.
     * @param repairedKeys Repaired entries.
     */
    void appendRepairedEntries(
        String cacheName,
        int partId,
        Map<VersionedKey, RepairMeta> repairedKeys);

    /**
     * Returns the total number of repaired entries.
     *
     * @return The total number of repaired entries.
     */
    long repairedEntriesSize();

    /**
     * Returns the total number of repaired entries for the specified cache.
     *
     * @param cacheName Cache name.
     * @return The total number of repaired entries for the specified cache.
     */
    long repairedEntriesSize(String cacheName);

    /**
     * This method is called when the given partition is completely processed.
     *
     * @param cacheName Cache name.
     * @param partId    Partition id.
     */
    void onPartitionProcessed(String cacheName, int partId);

    /**
     * Returns partition reconciliation result.
     *
     * @return Partition reconciliation result.
     */
    ReconciliationAffectedEntries result();

    /**
     * Flushes collected result to a file.
     *
     * @param startTime Start time.
     * @return File which contains the result or {@code null} if there are no insistent keys.
     */
    File flushResultsToFile(LocalDateTime startTime);

    /**
     * Represents a collector of inconsistent and repaired entries.
     */
    static class Simple implements ReconciliationResultCollector {
        public static final RowMetaComparator ROW_META_COMPARATOR = new RowMetaComparator();

        /** Ignite instance. */
        protected final IgniteEx ignite;

        /** Logger. */
        protected final IgniteLogger log;

        /** Indicates that sensitive information should be stored. */
        protected final boolean includeSensitive;

        /** Sensitive mode. */
        protected final GridToStringBuilder.SensitiveDataLogging sensitiveDataLogging;

        /** Root folder. */
        protected final File reconciliationDir;

        /**
         * Keys that were detected as inconsistent during the reconciliation process.
         * Cache name -> {Partition identifier -> set of inconsistent keys. }
         */
        protected final Map<String, Map<Integer, TreeSet<PartitionReconciliationDataRowMeta>>> inconsistentKeys = new HashMap<>();

        /** Entries that were detected as inconsistent but weren't repaired due to some reason. */
        protected final Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>> skippedEntries = new HashMap<>();

        /** Custom comparator for {@link PartitionReconciliationDataRowMeta}. It only compares binary representation of keys. */
        public static class RowMetaComparator implements Comparator<PartitionReconciliationDataRowMeta> {
            /** {@inheritDoc} */
            @Override public int compare(PartitionReconciliationDataRowMeta o1, PartitionReconciliationDataRowMeta o2) {
                return U.compareByteArrays(o1.keyMeta().binaryView(), o2.keyMeta().binaryView());
            }
        }

        /**
         * Creates a new SimpleCollector.
         *
         * @param ignite Ignite instance.
         * @param log Ignite logger.
         * @param includeSensitive {@code true} if sensitive information should be preserved.
         * @param sensitiveMode Sensitive mode.
         * @throws IgniteCheckedException If reconciliation parent directory cannot be created/resolved.
         */
        public Simple(
            IgniteEx ignite,
            IgniteLogger log,
            boolean includeSensitive,
            SensitiveMode sensitiveMode
        ) throws IgniteCheckedException {
            this.ignite = ignite;
            this.log = log;
            this.includeSensitive = includeSensitive;

            if (includeSensitive) {
                switch (sensitiveMode) {
                    case HASH:
                        sensitiveDataLogging = GridToStringBuilder.SensitiveDataLogging.HASH;
                        break;
                    case PLAIN:
                        sensitiveDataLogging = GridToStringBuilder.SensitiveDataLogging.PLAIN;
                        break;
                    default:
                        sensitiveDataLogging = GridToStringBuilder.getSensitiveDataLogging();
                }
            }
            else
                sensitiveDataLogging = GridToStringBuilder.SensitiveDataLogging.NONE;

            synchronized (ReconciliationResultCollector.class) {
                reconciliationDir = new File(U.defaultWorkDirectory() + separatorChar + ConsistencyCheckUtils.RECONCILIATION_DIR);

                if (!reconciliationDir.exists()) {
                    // TODO returned value is ignored
                    reconciliationDir.mkdir();
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void appendSkippedEntries(
            String cacheName,
            int partId,
            Map<VersionedKey, Map<UUID, VersionedValue>> keys
        ) {
            IgniteInternalCache<?, ?> cachex = ignite.cachex(cacheName);

            if (cachex == null)
                throw new IgniteException("Cache not found (was stopped) [name=" + cacheName + ']');

            CacheObjectContext ctx = cachex.context().cacheObjectContext();

            synchronized (skippedEntries) {
                Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>> data = new HashSet<>();

                for (VersionedKey keyVersion : keys.keySet()) {
                    try {
                        byte[] bytes = keyVersion.key().valueBytes(ctx);
                        String strVal = objectStringView(
                            ctx,
                            keyVersion.key().value(ctx, false),
                            sensitiveDataLogging);

                        PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta> holder
                            = new PartitionReconciliationSkippedEntityHolder<>(
                                new PartitionReconciliationKeyMeta(bytes, strVal),
                                KEY_WAS_NOT_REPAIRED
                        );

                        data.add(holder);
                    }
                    catch (Exception e) {
                        log.error("Serialization problem.", e);
                    }
                }

                skippedEntries
                    .computeIfAbsent(cacheName, k -> new HashMap<>())
                    .computeIfAbsent(partId, l -> new HashSet<>())
                    .addAll(data);
            }
        }

        /** {@inheritDoc} */
        @Override public void appendSkippedPartition(String cacheName, int partId) {
            Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>> data = new HashSet<>();

            data.add(new PartitionReconciliationSkippedEntityHolder<>(
                    new PartitionReconciliationKeyMeta(EMPTY_BYTES, "All partition keys."),
                LOST_PARTITION
            ));

            synchronized (skippedEntries) {
                skippedEntries
                    .computeIfAbsent(cacheName, k -> new HashMap<>())
                    .computeIfAbsent(partId, l -> new HashSet<>())
                    .addAll(data);
            }
        }

        /** {@inheritDoc} */
        @Override public void appendConflictedEntries(
            String cacheName,
            int partId,
            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts,
            Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys
        ) {
            IgniteInternalCache<?, ?> cachex = ignite.cachex(cacheName);

            if (cachex == null)
                throw new IgniteException("Cache not found (was stopped) [name=" + cacheName + ']');

            CacheObjectContext ctx = cachex.context().cacheObjectContext();

            synchronized (inconsistentKeys) {
                try {
                    inconsistentKeys.computeIfAbsent(cacheName, k -> new HashMap<>())
                        .computeIfAbsent(partId, k -> new TreeSet<>(ROW_META_COMPARATOR))
                        .addAll(mapPartitionReconciliation(conflicts, actualKeys, ctx, sensitiveDataLogging));
                }
                catch (IgniteCheckedException e) {
                    log.error("Broken key can't be added to result. ", e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public long conflictedEntriesSize() {
            synchronized (inconsistentKeys) {
                return inconsistentKeys
                    .keySet()
                    .stream()
                    .mapToLong(this::conflictedEntriesSize)
                    .sum();
            }
        }

        /** {@inheritDoc} */
        @Override public long conflictedEntriesSize(String cacheName) {
            synchronized (inconsistentKeys) {
                return inconsistentKeys
                    .getOrDefault(cacheName, Collections.emptyMap())
                    .values()
                    .stream()
                    .mapToLong(Set::size)
                    .sum();
            }
        }

        /** {@inheritDoc} */
        @Override public long repairedEntriesSize() {
            synchronized (inconsistentKeys) {
                return inconsistentKeys
                    .keySet()
                    .stream()
                    .mapToLong(this::repairedEntriesSize)
                    .sum();
            }
        }

        /** {@inheritDoc} */
        @Override public long repairedEntriesSize(String cacheName) {
            synchronized (inconsistentKeys) {
                return inconsistentKeys
                    .getOrDefault(cacheName, Collections.emptyMap())
                    .values()
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(r -> r.repairMeta() != null && r.repairMeta().fixed())
                    .count();
            }
        }

        /** {@inheritDoc} */
        @Override public void appendRepairedEntries(
            String cacheName,
            int partId,
            Map<VersionedKey, RepairMeta> repairedKeys
        ) {
            IgniteInternalCache<?, ?> cachex = ignite.cachex(cacheName);

            if (cachex == null)
                throw new IgniteException("Cache not found (was stopped) [name=" + cacheName + ']');

            CacheObjectContext ctx = cachex.context().cacheObjectContext();

            synchronized (inconsistentKeys) {
                try {
                    List<PartitionReconciliationDataRowMeta> res = new ArrayList<>();

                    for (Map.Entry<VersionedKey, RepairMeta> entry : repairedKeys.entrySet()) {
                        Map<UUID, PartitionReconciliationValueMeta> valMap = new HashMap<>();

                        for (Map.Entry<UUID, VersionedValue> uuidBasedEntry : entry.getValue().getPreviousValue().entrySet()) {
                            Optional<CacheObject> cacheObjOpt = Optional.ofNullable(uuidBasedEntry.getValue().value());

                            valMap.put(
                                uuidBasedEntry.getKey(),
                                cacheObjOpt.isPresent() ?
                                    new PartitionReconciliationValueMeta(
                                        cacheObjOpt.get().valueBytes(ctx),
                                        cacheObjOpt.map(o -> objectStringView(ctx, o, sensitiveDataLogging)).orElse(null),
                                        uuidBasedEntry.getValue().version())
                                    :
                                    null);
                        }

                        KeyCacheObject key = entry.getKey().key();

                        key.finishUnmarshal(ctx, null);

                        RepairMeta repairMeta = entry.getValue();

                        Optional<CacheObject> cacheObjRepairValOpt = Optional.ofNullable(repairMeta.value());

                        res.add(
                            new PartitionReconciliationDataRowMeta(
                                new PartitionReconciliationKeyMeta(
                                    key.valueBytes(ctx),
                                    objectStringView(ctx, key, sensitiveDataLogging)),
                                valMap,
                                new PartitionReconciliationRepairMeta(
                                    repairMeta.fixed(),
                                    cacheObjRepairValOpt.isPresent() ?
                                        new PartitionReconciliationValueMeta(
                                            cacheObjRepairValOpt.get().valueBytes(ctx),
                                            cacheObjRepairValOpt.map(o -> objectStringView(ctx, o, sensitiveDataLogging)).orElse(null),
                                            null)
                                        :
                                        null,
                                    repairMeta.repairAlg())));
                    }

                    inconsistentKeys.computeIfAbsent(cacheName, k -> new HashMap<>())
                        .computeIfAbsent(partId, k -> new TreeSet<>(ROW_META_COMPARATOR))
                        .addAll(res);
                }
                catch (IgniteCheckedException e) {
                    log.error("Broken key can't be added to result. ", e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onPartitionProcessed(String cacheName, int partId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ReconciliationAffectedEntries result() {
            synchronized (inconsistentKeys) {
                synchronized (skippedEntries) {
                    // This copy is need to avoid RU incompatible changes.
                    Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> copy = new HashMap<>(inconsistentKeys.size());

                    for (Map.Entry<String, Map<Integer, TreeSet<PartitionReconciliationDataRowMeta>>> e : inconsistentKeys.entrySet()) {
                        Map<Integer, List<PartitionReconciliationDataRowMeta>> c = new HashMap<>(e.getValue().size());

                        for (Map.Entry<Integer, TreeSet<PartitionReconciliationDataRowMeta>> e0 : e.getValue().entrySet())
                            c.put(e0.getKey(), new ArrayList<>(e0.getValue()));

                        copy.put(e.getKey(), c);
                    }

                    return new ReconciliationAffectedEntries(
                        collectNodeIdToConsistentIdMapping(ignite),
                        copy,
                        skippedEntries
                    );
                }
            }
        }

        Map<UUID, String> collectNodeIdToConsistentIdMapping(Ignite ignite) {
            return ignite.cluster().nodes().stream().collect(Collectors.toMap(
                ClusterNode::id,
                n -> n.consistentId().toString())
            );
        }

        /** {@inheritDoc} */
        @Override public File flushResultsToFile(LocalDateTime startTime) {
            ReconciliationAffectedEntries res = result();

            if (res != null && !res.isEmpty()) {
                try {
                    File file = createLocalResultFile(ignite.context().discovery().localNode(), startTime);

                    try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file)))) {
                        res.print(pw::write, includeSensitive);

                        pw.flush();

                        return file;
                    }
                    catch (IOException e) {
                        log.error("Unable to write report to file " + e.getMessage());
                    }
                }
                catch (IgniteCheckedException | IOException e) {
                    log.error("Unable to create file " + e.getMessage());
                }
            }

            return null;
        }
    }

    /**
     * Represents a collector of inconsistent and repaired entries that persists temporary results (per partition).
     */
    static class Compact extends Simple {
        /** Total number of inconsistent keys. */
        private final AtomicInteger totalInconsistentKeys = new AtomicInteger();

        /** Total number of repaired keys. */
        private final AtomicInteger totalRepairedKeys = new AtomicInteger();

        /** Mapping of cache name to the number of inconsistent keys. */
        private final Map<String, Long> inconsistentKeysPerCache = new HashMap<>();

        /** Mapping of cache name to the number of repaired keys. */
        private final Map<String, Long> repairedKeysPerCache = new HashMap<>();

        /** Total number of skipped entities. */
        private final AtomicInteger totalSkippedCnt = new AtomicInteger();

        /** Provides mapping of cache name to temporary filename with results. */
        private final Map<String, String> tmpFiles = new ConcurrentHashMap<>();

        /** Mapping of node Ids to consistent Ids. */
        private final Map<UUID, String> nodesIdsToConsistentIdsMap = ignite.cluster().nodes().stream()
            .collect(Collectors.toMap(ClusterNode::id, n -> n.consistentId().toString()));

        /** Session identifier. */
        private final long sesId;

        /**
         * Creates a new collector.
         *
         * @param ignite Ignite instance.
         * @param log Ignite logger.
         * @param includeSensitive {@code true} if sensitive information should be preserved.
         * @param sensitiveMode Sensitive mode.
         * @throws IgniteCheckedException If reconciliation parent directory cannot be created/resolved.
         */
        Compact(
            IgniteEx ignite,
            IgniteLogger log,
            long sesId,
            boolean includeSensitive,
            SensitiveMode sensitiveMode
        ) throws IgniteCheckedException {
            super(ignite, log, includeSensitive, sensitiveMode);

            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public long conflictedEntriesSize(String cacheName) {
            synchronized (inconsistentKeys) {
                Long conflicts = inconsistentKeysPerCache.getOrDefault(cacheName, 0L);

                return conflicts + super.conflictedEntriesSize(cacheName);
            }
        }

        /** {@inheritDoc} */
        @Override public long repairedEntriesSize() {
            return totalRepairedKeys.get() + super.repairedEntriesSize();
        }

        /** {@inheritDoc} */
        @Override public long repairedEntriesSize(String cacheName) {
            synchronized (inconsistentKeys) {
                Long repaired = repairedKeysPerCache.getOrDefault(cacheName, 0L);

                return repaired + super.repairedEntriesSize(cacheName);
            }
        }

        /** {@inheritDoc} */
        @Override public void onPartitionProcessed(String cacheName, int partId) {
            if (log.isDebugEnabled())
                log.debug("Partition has been processed [cacheName=" + cacheName + ", partId=" + partId + ']');

            TreeSet<PartitionReconciliationDataRowMeta> meta = null;
            Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>> skipped = null;

            synchronized (inconsistentKeys) {
                Map<Integer, TreeSet<PartitionReconciliationDataRowMeta>> c = inconsistentKeys.get(cacheName);
                if (c != null)
                    meta = c.remove(partId);

                if (!F.isEmpty(meta)) {
                    totalInconsistentKeys.addAndGet(meta.size());

                    inconsistentKeysPerCache.merge(cacheName, (long) meta.size(), Long::sum);

                    long repairedKeysCnt = meta.stream().filter(m -> m.repairMeta() != null && m.repairMeta().fixed()).count();

                    repairedKeysPerCache.merge(cacheName, repairedKeysCnt, Long::sum);
                }
            }

            synchronized (skippedEntries) {
                Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>> c = skippedEntries.get(cacheName);
                if (c != null)
                    skipped = c.remove(partId);

                if (!F.isEmpty(skipped))
                    totalSkippedCnt.addAndGet(skipped.size());
            }

            if (!F.isEmpty(meta) || !F.isEmpty(skipped))
                storePartition(cacheName, partId, meta, skipped);
        }

        /** {@inheritDoc} */
        @Override public ReconciliationAffectedEntries result() {
            synchronized (inconsistentKeys) {
                synchronized (skippedEntries) {
                    return new ReconciliationAffectedEntriesExtended(
                        collectNodeIdToConsistentIdMapping(ignite),
                        totalInconsistentKeys.get(),
                        0, // skipped caches count which is not tracked/used.
                        totalSkippedCnt.get());
                }
            }
        }

        /** {@inheritDoc} */
        @Override public File flushResultsToFile(LocalDateTime startTime) {
            if (!tmpFiles.isEmpty()) {
                try {
                    File file = createLocalResultFile(ignite.context().discovery().localNode(), startTime);

                    try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file)))) {
                        printFileHead(out, totalInconsistentKeys.get(), totalSkippedCnt.get());

                        for (Map.Entry<String, String> e : tmpFiles.entrySet()) {
                            out.println(e.getKey());

                            try (BufferedReader reader = new BufferedReader(new FileReader(e.getValue()))) {
                                String line;
                                while ((line = reader.readLine()) != null)
                                    out.println(line);
                            }

                            Files.delete(Paths.get(e.getValue()));
                        }

                        out.flush();

                        return file;
                    }
                    catch (IOException e) {
                        log.error("Unable to write report to file " + e.getMessage());
                    }
                }
                catch (IgniteCheckedException | IOException e) {
                    log.error("Unable to create file " + e.getMessage());
                }
            }

            return null;
        }

        /**
         * Prints common information.
         *
         * @param out Output stream.
         * @param inconsistentKeyCnt Number of inconsistent keys.
         * @param skippedEntitiesCnt Number of skipped entities.
         */
        private void printFileHead(PrintWriter out, int inconsistentKeyCnt, int skippedEntitiesCnt) {
            out.println();
            out.println("INCONSISTENT KEYS: " + inconsistentKeyCnt);
            out.println();
            out.println("SKIPPED ENTRIES: " + skippedEntitiesCnt);
            out.println();

            out.println("<cacheName>");
            out.println("\t<partitionId>");
            out.println("\t\t<key>");
            out.println("\t\t\t<nodeConsistentId>, <nodeId>: <value> <version>");
            out.println("\t\t\t...");
            out.println("\t\t\t<info on whether conflict is fixed>");
            out.println();
        }

        /**
         * Stores the result of partition reconciliation for the given partition.
         *
         * @param cacheName Cache name.
         * @param partId Partition id.
         * @param meta Inconsistent entries.
         * @param skipped Skipped entities.
         */
        private void storePartition(
            String cacheName,
            int partId,
            TreeSet<PartitionReconciliationDataRowMeta> meta,
            Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>> skipped
        ) {
            String maskId = U.maskForFileName(ignite.context().discovery().localNode().consistentId().toString());

            String fileName = tmpFiles.computeIfAbsent(cacheName, d -> {
                File file = new File(reconciliationDir.getPath() + separatorChar + maskId + '-' + sesId + '-' + cacheName + ".txt");
                try {
                    // TODO handle a returned value
                    file.createNewFile();
                }
                catch (IOException e) {
                    log.error("Cannot create a file for storing partition's data " +
                        "[cacheName=" + cacheName + ", partId" + partId + ']');

                    return null;
                }

                return file.getAbsolutePath();
            });

            // Cannot create temporary file. Error message is already logged.
            if (fileName == null)
                return;

            synchronized (fileName) {
                try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
                    out.print('\t');
                    out.println(partId);

                    if (!F.isEmpty(meta)) {
                        for (PartitionReconciliationDataRowMeta row : meta)
                            out.print(getConflictsAsString(row, nodesIdsToConsistentIdsMap, includeSensitive));
                    }

                    if (!F.isEmpty(skipped)) {
                        for (PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta> keyMeta : skipped)
                            out.print(ReconciliationAffectedEntries.getSkippedAsString(keyMeta, includeSensitive, true));
                    }
                }
                catch (IOException e) {
                    log.error("Cannot store partition's data [cacheName=" + cacheName + ", partId" + partId + ']');
                }
            }
        }
    }
}
