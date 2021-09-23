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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.RECONCILIATION_DIR;

/**
 * Tests per cache output of partition reconciliation utility (aka compact mode).
 */
public class PartitionReconciliationCompactCollectorTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    private static final int NODES_CNT = 3;

    /** Number of partitions. */
    private static final int PARTS = 12;

    /** Keys count. */
    private static final int KEYS_CNT = PARTS * 10;

    /** Caches count. */
    private static final int CACHES_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(300L * 1024 * 1024))
        );

        CacheConfiguration<Integer, Integer>[] ccfgs = new CacheConfiguration[CACHES_CNT];
        for (int i = 0; i < ccfgs.length; ++i) {
            CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();
            ccfg.setName(DEFAULT_CACHE_NAME + '-' + i);
            ccfg.setAtomicityMode(ATOMIC);
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
            ccfg.setBackups(NODES_CNT - 1);

            ccfgs[i] = ccfg;
        }
        cfg.setCacheConfiguration(ccfgs);

        cfg.setConsistentId(name);

        cfg.setAutoActivationEnabled(false);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();
        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), RECONCILIATION_DIR, false));

        cleanPersistenceDir();
    }

    /**
     * Tests that both modes (locOutput and compact) of execution of partition reconciliation utility provide the same result.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCompactMode() throws Exception {
        startGrids(NODES_CNT);

        grid(0).cluster().active(true);

        Set<String> cacheNames = IntStream.range(0, CACHES_CNT)
            .mapToObj(i -> DEFAULT_CACHE_NAME + '-' + i).collect(Collectors.toSet());

        cacheNames.forEach(cacheName -> {
            IgniteCache<Object, Object> cache = grid(0).cache(cacheName);

            for (int i = 0; i < KEYS_CNT; i++)
                cache.put(i, String.valueOf(i));

            GridCacheContext[] nodeCacheCtxs = new GridCacheContext[NODES_CNT];

            for (int i = 0; i < NODES_CNT; i++)
                nodeCacheCtxs[i] = grid(i).cachex(cacheName).context();

            for (int i = 0; i < KEYS_CNT; i++) {
                if (i % 3 == 0)
                    simulateMissingEntryCorruption(nodeCacheCtxs[i % NODES_CNT], i);
                else
                    simulateOutdatedVersionCorruption(nodeCacheCtxs[i % NODES_CNT], i);
            }
        });

        updatePartitionsSize(grid(0), DEFAULT_CACHE_NAME + '-' + 0);
        updatePartitionsSize(grid(1), DEFAULT_CACHE_NAME + '-' + 1);

        ReconciliationResult locOutputRes = partitionReconciliation(
            grid(0),
            new VisorPartitionReconciliationTaskArg.Builder()
                .locOutput(true)
                .repair(false)
                .repairAlg(RepairAlgorithm.PRINT_ONLY));

        Map<String, Map<Integer, String>> parsedLocOutput = parseDataConsistencyResult(locOutputRes, cacheNames);
        Map<String, Map<String, Map<String, List<String>>>> parsedSizesLocOutput = parseSizeConsistensyResult(locOutputRes, cacheNames);

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), RECONCILIATION_DIR, false));

        ReconciliationResult compactRes = partitionReconciliation(
            grid(0),
            new VisorPartitionReconciliationTaskArg.Builder()
                .locOutput(false)
                .repair(false)
                .repairAlg(RepairAlgorithm.PRINT_ONLY));

        Map<String, Map<Integer, String>> parsedCompactOutput = parseDataConsistencyResult(compactRes, cacheNames);
        Map<String, Map<String, Map<String, List<String>>>> parsedSizesCompactOutput = parseSizeConsistensyResult(compactRes, cacheNames);

        assertEquals(parsedLocOutput, parsedCompactOutput);
        assertEquals(parsedSizesLocOutput, parsedSizesCompactOutput);
    }

    /**
     * Parses partition reconciliation file and returns mapping
     * cacheName -> Map partId -> string representation of inconsistent keys.
     *
     * @param res Partition reconciliation result.
     * @param cacheNames Set of cache names.
     * @return Parsed result based on partition reconciliation file(s).
     */
    private Map<String, Map<Integer, String>> parseDataConsistencyResult(ReconciliationResult res, Set<String> cacheNames) {
        Map<String, Map<Integer, String>> ret = new HashMap<>();

        res.nodeIdToFolder().forEach((k, v) -> {
            try {
                Map<String, Map<Integer, String>> r = parseDataConsistencyResult(v, cacheNames);

                r.forEach((c, p) -> {
                    if (ret.containsKey(c))
                        ret.get(c).putAll(p);
                    else
                        ret.put(c, p);
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return ret;
    }

    /**
     * Parses partition reconciliation file and returns mapping
     * cacheName -> partId ->  nodeId -> old and new size.
     *
     * @param res Partition reconciliation result.
     * @param cacheNames Set of cache names.
     * @return Parsed result based on partition reconciliation file(s).
     */
    private Map<String, Map<String, Map<String, List<String>>>> parseSizeConsistensyResult(ReconciliationResult res, Set<String> cacheNames) {
        Map<String, Map<String, Map<String, List<String>>>> ret = new HashMap<>();

        res.nodeIdToFolder().forEach((k, v) -> {
            try {
                Map<String, Map<String, Map<String, List<String>>>> r = parseSizeConsistencyResult(v, cacheNames);

                r.forEach((c, p) -> {
                    if (ret.containsKey(c))
                        ret.get(c).putAll(p);
                    else
                        ret.put(c, p);
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return ret;
    }

    /**
     * Parses partition reconciliation file and returns mapping
     * cacheName -> Map partId -> string representation of inconsistent keys.
     *
     * @param pathToReport Path to report.
     * @param cacheNames Set of cache names.
     * @return Parsed result based on partition reconciliation file(s).
     * @throws IOException If failed.
     */
    private Map<String, Map<Integer, String>> parseDataConsistencyResult(String pathToReport, Set<String> cacheNames) throws IOException {
        assertNotNull("Partition reconciliation file is not created.", pathToReport);
        assertTrue("Cannot find partition reconciliation file.", new File(pathToReport).exists());

        Map<String, Map<Integer, String>> ret = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(pathToReport))) {
            String line;

            while ((line = reader.readLine()) != null) {
                // skip header
                if (cacheNames.contains(line))
                    break;
            }

            if (line == null || line.contains("PARTITIONS WITH BROKEN SIZE"))
                return ret;

            String cacheName = line;

            line = reader.readLine();
            if (line == null)
                throw new IllegalStateException("empty cache should not be logged.");

            Integer partId = Integer.valueOf(line.substring(1));
            ret.put(cacheName, new HashMap<>());
            Set<String> partKeys = new HashSet<>();

            while ((line = reader.readLine()) != null) {
                if (line.contains("PARTITIONS WITH BROKEN SIZE") || line.isEmpty())
                    break;

                if (line.startsWith("\t\t\t")) {
                }
                else if (line.startsWith("\t\t"))
                    partKeys.add(line);
                else {
                    if (line.startsWith("\t")) {
                        // new partition
                        ret.get(cacheName).put(partId, String.join("\n", partKeys));
                        partKeys.clear();
                        partId = Integer.valueOf(line.substring(1));
                    }
                    else {
                        // new cache
                        ret.get(cacheName).put(partId, String.join("\n", partKeys));
                        partKeys.clear();
                        ret.put((cacheName = line), new HashMap<>());
                    }
                }
            }

            ret.get(cacheName).put(partId, String.join("\n", partKeys));
        }

        return ret;
    }

    /**
     * Parses partition reconciliation file and returns mapping
     * cacheName -> partId ->  nodeId -> old and new size.
     *
     * @param pathToReport Path to report.
     * @param cacheNames Set of cache names.
     * @return Parsed result based on partition reconciliation file(s).
     * @throws IOException If failed.
     */
    private Map<String, Map<String, Map<String, List<String>>>> parseSizeConsistencyResult(String pathToReport, Set<String> cacheNames) throws IOException {
        assertNotNull("Partition reconciliation file is not created.", pathToReport);
        assertTrue("Cannot find partition reconciliation file.", new File(pathToReport).exists());

        Map<String, Map<String, Map<String, List<String>>>> ret = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(pathToReport))) {
            String line;

            boolean sizesReached = false;

            while ((line = reader.readLine()) != null) {
                // skip header and data reconciliation
                if (line.contains("PARTITIONS WITH BROKEN SIZE")) {
                    sizesReached = true;

                    continue;
                }
                else if (!sizesReached || line.isEmpty())
                    continue;

                String cacheName;

                while (line != null && !line.isEmpty()) {
                    cacheName = line;

                    ret.put(cacheName, new HashMap<>());

                    Pattern p = Pattern.compile("\\d+");

                    while ((line = reader.readLine()) != null && !line.isEmpty()) {

                        if (cacheNames.contains(line))
                            break;

                        String partId = line.substring(1);
                        String node = reader.readLine().substring(2);
                        String sizes = reader.readLine().substring(3);

                        Matcher m = p.matcher(sizes);
                        List<String> oldAndNewSize = new ArrayList<>();
                        m.find();
                        oldAndNewSize.add(m.group());
                        m.find();
                        oldAndNewSize.add(m.group());

                        ret.get(cacheName).put(partId, new HashMap<>());
                        ret.get(cacheName).get(partId).put(node, oldAndNewSize);
                    }
                }
            }
        }

        return ret;
    }
}
