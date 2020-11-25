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

package org.apache.ignite.internal.util.tostring;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.SensitiveDataLogging.*;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Class for checking sensitive data when outputting transactions to the log.
 */
public class TransactionSensitiveDataTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** Node count. */
    private static final int NODE_COUNT = 2;

    /** Create a client node. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        setFieldValue(GridNearTxPrepareFutureAdapter.class, "log", null);
        ((AtomicReference<IgniteLogger>)getFieldValue(GridNearTxPrepareFutureAdapter.class, "logRef")).set(null);

        clearGridToStringClassCache();

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLog.clearListeners();

        stopAllGrids();

        clearGridToStringClassCache();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setGridLogger(testLog)
            .setClientMode(client)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setBackups(NODE_COUNT)
                    .setAffinity(new RendezvousAffinityFunction(false, 10))
            );
    }

    /**
     * Test for checking the absence of sensitive data in log during an
     * exchange while an active transaction is running.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    @Test
    public void testHideSensitiveDataDuringExchange() throws Exception {
        checkSensitiveDataDuringExchange();
    }

    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    @Test
    public void testHashSensitiveDataDuringExchange() throws Exception {
        checkSensitiveDataDuringExchange();
    }

    /**
     * Test for checking the presence of sensitive data in log during an
     * exchange while an active transaction is running.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    @Test
    public void testShowSensitiveDataDuringExchange() throws Exception {
        checkSensitiveDataDuringExchange();
    }

    /**
     * Test for checking the absence of sensitive data in log when node exits
     * during transaction preparation.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    @Test
    public void testHideSensitiveDataDuringNodeLeft() throws Exception {
        checkSensitiveDataDuringNodeLeft();
    }

    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    @Test
    public void testHashSensitiveDataDuringNodeLeft() throws Exception {
        checkSensitiveDataDuringNodeLeft();
    }

    /**
     * Test for checking the presence of sensitive data in log when node exits
     * during transaction preparation.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    @Test
    public void testShowSensitiveDataDuringNodeLeft() throws Exception {
        checkSensitiveDataDuringNodeLeft();
    }

    /**
     * Receiving a log message "Partition release future:" during the exchange
     * to check whether or not sensitive data is in printed transactions.
     *
     * @param check Check sensitive data in log message.
     * @throws Exception If failed.
     */
    private void checkSensitiveDataDuringExchange() throws Exception {
        IgniteEx crd = startGrids(NODE_COUNT);

        awaitPartitionMapExchange();

        AtomicReference<String> strToCheckRef = new AtomicReference<>();

        LogListener logLsnr = LogListener.matches(logStr -> {
            if (logStr.contains("Partition release future:") && currentThread().getName().contains(crd.name())) {
                strToCheckRef.set(logStr);

                return true;
            }

            return false;
        }).build();

        testLog.registerListener(logLsnr);

        IgniteCache<Object, Object> cache = crd.getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary();

        IgniteBinary binary = crd.binary();

        BinaryObject binKey = binary.toBinary(new Key(0));
        BinaryObject binPerson = binary.toBinary(new Person(1, "name_1"));

        cache.put(binKey, binPerson);

        Transaction tx = crd.transactions().txStart();

        cache.put(binKey, binPerson);

        GridTestUtils.runAsync(() -> {
            logLsnr.check(10 * crd.configuration().getNetworkTimeout());

            tx.commit();

            return null;
        });

        startGrid(NODE_COUNT);

        if (S.getSensitiveDataLogging() == PLAIN/*includeSensitive*/) {
            assertContains(log, maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binKey, Key.class)));

            assertContains(log, maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binPerson, Person.class)));
        }
        else {
            Pattern patternKey;
            Pattern patternVal;

            if (S.getSensitiveDataLogging() == HASH) {
                patternKey = Pattern.compile("(IgniteTxKey \\[key=" + binKey.hashCode() + ", cacheId=\\d+\\])");

                patternVal = Pattern.compile("(TxEntryValueHolder \\[val=" + binPerson.hashCode() + ", op=\\D+\\])");
            }
            else {
                patternKey = Pattern.compile("(IgniteTxKey \\[cacheId=\\d+\\])");

                patternVal = Pattern.compile("(TxEntryValueHolder \\[op=\\D+\\])");
            }

            final String strToCheck = maskIdHash(strToCheckRef.get());

            Matcher matcherKey = patternKey.matcher(strToCheck);

            assertTrue(strToCheck, matcherKey.find());

            Matcher matcherVal = patternVal.matcher(strToCheck);

            assertTrue(strToCheck, matcherVal.find());
        }

//        assertContains(log, maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binPerson, Person.class)));

//        check0.accept(maskIdHash(strToCheckRef.get()), String.valueOf(binKey.hashCode())/*maskIdHash(toStr(binKey, Key.class))*/);
//        check1.accept(maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binPerson, Person.class)));
    }
// withSensitive
// Partition release future: PartitionReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[ExplicitLockReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]], AtomicUpdateReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]], DataStreamerReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]], LocalTxReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[TxFinishFuture [tx=GridNearTxLocal [mappings=IgniteTxMappingsImpl [], nearLocallyMapped=false, colocatedLocallyMapped=true, needCheckBackup=null, hasRemoteLocks=false, trackTimeout=false, systemTime=19652117, systemStartTime=0, prepareStartTime=0, prepareTime=0, commitOrRollbackStartTime=0, commitOrRollbackTime=0, lb=null, mvccOp=null, qryId=-1, crdVer=0, thread=test-runner-#1%tostring.TransactionSensitiveDataTest%, mappings=IgniteTxMappingsImpl [], super=GridDhtTxLocalAdapter [nearOnOriginatingNode=false, nearNodes=KeySetView [], dhtNodes=KeySetView [], explicitLock=false, super=IgniteTxLocalAdapter [completedBase=null, sndTransformedVals=false, depEnabled=false, txState=IgniteTxStateImpl [activeCacheIds=[1544803905], recovery=false, mvccEnabled=false, mvccCachingCacheIds=[], txMap=ArrayList [IgniteTxEntry [txKey=IgniteTxKey [key=o.a.i.i.util.tostring.TransactionSensitiveDataTest$Key [idHash=1288853135, hash=31399714, id=0], cacheId=1544803905], val=TxEntryValueHolder [val=o.a.i.i.util.tostring.TransactionSensitiveDataTest$Person [idHash=1363645311, hash=104653023, orgId=1, name=name_1], op=UPDATE], prevVal=TxEntryValueHolder [val=o.a.i.i.util.tostring.TransactionSensitiveDataTest$Person [idHash=1363645311, hash=104653023, orgId=1, name=name_1], op=UPDATE], oldVal=TxEntryValueHolder [val=null, op=NOOP], entryProcessorsCol=null, ttl=-1, conflictExpireTime=-1, conflictVer=null, explicitVer=null, dhtVer=null, filters=CacheEntryPredicate[] [], filtersPassed=false, filtersSet=true, entry=GridDhtCacheEntry [rdrs=ReaderId[] [], part=4, super=GridDistributedCacheEntry [super=GridCacheMapEntry [key=o.a.i.i.util.tostring.TransactionSensitiveDataTest$Key [idHash=1288853135, hash=31399714, id=0], val=o.a.i.i.util.tostring.TransactionSensitiveDataTest$Person [idHash=1565381247, hash=104653023, orgId=1, name=name_1], ver=GridCacheVersion [topVer=217594885, order=1606114883813, nodeOrder=1], hash=31399714, extras=GridCacheMvccEntryExtras [mvcc=GridCacheMvcc [locs=LinkedList [GridCacheMvccCandidate [nodeId=79d70802-7d44-4135-a6f1-5bc160400000, ver=GridCacheVersion [topVer=217594885, order=1606114883814, nodeOrder=1], threadId=13, id=4, topVer=AffinityTopologyVersion [topVer=2, minorTopVer=1], reentry=null, otherNodeId=79d70802-7d44-4135-a6f1-5bc160400000, otherVer=GridCacheVersion [topVer=217594885, order=1606114883814, nodeOrder=1], mappedDhtNodes=null, mappedNearNodes=null, ownerVer=null, serOrder=null, key=o.a.i.i.util.tostring.TransactionSensitiveDataTest$Key [idHash=1288853135, hash=31399714, id=0], masks=local=1|owner=1|ready=1|reentry=0|used=0|tx=1|single_implicit=0|dht_local=1|near_local=0|removed=0|read=0, prevVer=null, nextVer=null]], rmts=null]], flags=2]]], prepared=0, locked=true, nodeId=79d70802-7d44-4135-a6f1-5bc160400000, locMapped=false, expiryPlc=null, transferExpiryPlc=false, flags=2, partUpdateCntr=0, serReadVer=null, xidVer=GridCacheVersion [topVer=217594885, order=1606114883814, nodeOrder=1]]]], super=IgniteTxAdapter [xidVer=GridCacheVersion [topVer=217594885, order=1606114883814, nodeOrder=1], writeVer=null, implicit=false, loc=true, threadId=13, startTime=1606114884844, nodeId=79d70802-7d44-4135-a6f1-5bc160400000, isolation=REPEATABLE_READ, concurrency=PESSIMISTIC, timeout=0, sysInvalidate=false, sys=false, plc=2, commitVer=null, finalizing=NONE, invalidParts=null, state=ACTIVE, timedOut=false, topVer=AffinityTopologyVersion [topVer=2, minorTopVer=1], mvccSnapshot=null, skipCompletedVers=false, parentTx=null, duration=20200ms, onePhaseCommit=false], size=1]]], completionTime=0, duration=20200]]], AllTxReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]]]]
// [txKey=IgniteTxKey [key=o.a.i.i.util.tostring.TransactionSensitiveDataTest$Key [idHash=1288853135, hash=31399714, id=0], cacheId=1544803905]
// withoutSensitive
// Partition release future: PartitionReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[ExplicitLockReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]], AtomicUpdateReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]], DataStreamerReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]], LocalTxReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[TxFinishFuture [tx=GridNearTxLocal [mappings=IgniteTxMappingsImpl [], nearLocallyMapped=false, colocatedLocallyMapped=true, needCheckBackup=null, hasRemoteLocks=false, trackTimeout=false, systemTime=14619740, systemStartTime=0, prepareStartTime=0, prepareTime=0, commitOrRollbackStartTime=0, commitOrRollbackTime=0, lb=null, mvccOp=null, qryId=-1, crdVer=0, thread=test-runner-#1%tostring.TransactionSensitiveDataTest%, mappings=IgniteTxMappingsImpl [], super=GridDhtTxLocalAdapter [nearOnOriginatingNode=false, nearNodes=KeySetView [], dhtNodes=KeySetView [], explicitLock=false, super=IgniteTxLocalAdapter [completedBase=null, sndTransformedVals=false, depEnabled=false, txState=IgniteTxStateImpl [activeCacheIds=[1544803905], recovery=false, mvccEnabled=false, mvccCachingCacheIds=[], txMap=ArrayList [IgniteTxEntry [txKey=IgniteTxKey [key=31399714, cacheId=1544803905], val=TxEntryValueHolder [op=UPDATE], prevVal=TxEntryValueHolder [op=UPDATE], oldVal=TxEntryValueHolder [op=NOOP], entryProcessorsCol=null, ttl=-1, conflictExpireTime=-1, conflictVer=null, explicitVer=null, dhtVer=null, filters=CacheEntryPredicate[] [], filtersPassed=false, filtersSet=true, entry=GridDhtCacheEntry [rdrs=ReaderId[] [], part=4, super=GridDistributedCacheEntry [super=GridCacheMapEntry [ver=GridCacheVersion [topVer=217596866, order=1606116864796, nodeOrder=1], hash=31399714, extras=GridCacheMvccEntryExtras [mvcc=GridCacheMvcc [locs=LinkedList [GridCacheMvccCandidate [nodeId=36fbb1f8-eba3-4f9f-9a65-c2f65ae00000, ver=GridCacheVersion [topVer=217596866, order=1606116864797, nodeOrder=1], threadId=13, id=4, topVer=AffinityTopologyVersion [topVer=2, minorTopVer=1], reentry=null, otherNodeId=36fbb1f8-eba3-4f9f-9a65-c2f65ae00000, otherVer=GridCacheVersion [topVer=217596866, order=1606116864797, nodeOrder=1], mappedDhtNodes=null, mappedNearNodes=null, ownerVer=null, serOrder=null, masks=local=1|owner=1|ready=1|reentry=0|used=0|tx=1|single_implicit=0|dht_local=1|near_local=0|removed=0|read=0, prevVer=null, nextVer=null]], rmts=null]], flags=2]]], prepared=0, locked=true, nodeId=36fbb1f8-eba3-4f9f-9a65-c2f65ae00000, locMapped=false, expiryPlc=null, transferExpiryPlc=false, flags=2, partUpdateCntr=0, serReadVer=null, xidVer=GridCacheVersion [topVer=217596866, order=1606116864797, nodeOrder=1]]]], super=IgniteTxAdapter [xidVer=GridCacheVersion [topVer=217596866, order=1606116864797, nodeOrder=1], writeVer=null, implicit=false, loc=true, threadId=13, startTime=1606116865367, nodeId=36fbb1f8-eba3-4f9f-9a65-c2f65ae00000, isolation=REPEATABLE_READ, concurrency=PESSIMISTIC, timeout=0, sysInvalidate=false, sys=false, plc=2, commitVer=null, finalizing=NONE, invalidParts=null, state=ACTIVE, timedOut=false, topVer=AffinityTopologyVersion [topVer=2, minorTopVer=1], mvccSnapshot=null, skipCompletedVers=false, parentTx=null, duration=20188ms, onePhaseCommit=false], size=1]]], completionTime=0, duration=20188]]], AllTxReleaseFuture [topVer=AffinityTopologyVersion [topVer=3, minorTopVer=0], futures=[]]]]
// [txKey=IgniteTxKey [key=31399714, cacheId=1544803905]

    /**
     * Receiving the “Failed to send message to remote node” and
     * “Received error when future is done” message logs during the node exit
     * when preparing the transaction to check whether or not sensitive data
     * is in the printed transactions.
     *
     * @param check Check sensitive data in log messages.
     * @throws Exception If failed.
     */
    private void checkSensitiveDataDuringNodeLeft() throws Exception {
        client = false;

        startGrids(NODE_COUNT);

        client = true;

        IgniteEx clientNode = startGrid(NODE_COUNT);

        awaitPartitionMapExchange();

        AtomicReference<String> strFailedSndRef = new AtomicReference<>();
        AtomicReference<String> strReceivedErrorRef = new AtomicReference<>();

        testLog.registerListener(logStr -> {
            if (logStr.contains("Failed to send message to remote node"))
                strFailedSndRef.set(logStr);
        });

        testLog.registerListener(logStr -> {
            if (logStr.contains("Received error when future is done"))
                strReceivedErrorRef.set(logStr);
        });

        int stopGridId = 0;

        TestRecordingCommunicationSpi.spi(clientNode).closure((clusterNode, message) -> {
            if (GridNearTxPrepareRequest.class.isInstance(message))
                stopGrid(stopGridId);
        });

        String cacheName = DEFAULT_CACHE_NAME;

        IgniteCache<Object, Object> cache = clientNode.getOrCreateCache(cacheName).withKeepBinary();

        IgniteBinary binary = clientNode.binary();

        BinaryObject binKey = binary.toBinary(new Key(primaryKey(grid(stopGridId).cache(cacheName))));
        BinaryObject binPerson = binary.toBinary(new Person(1, "name_1"));

        try (Transaction tx = clientNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(binKey, binPerson);

            tx.commit();
        } catch (Exception ignored) {
            //ignore
        }

        String strFailedSndStr = maskIdHash(strFailedSndRef.get());
        String strReceivedErrorStr = maskIdHash(strReceivedErrorRef.get());

        String binKeyStr = maskIdHash(toStr(binKey, Key.class));
        String binPersonStr = maskIdHash(toStr(binPerson, Person.class));

//        check0.accept(strFailedSndStr, binKeyStr);
//        check1.accept(strFailedSndStr, binPersonStr);
//
//        check0.accept(strReceivedErrorStr, binKeyStr);
//        check1.accept(strReceivedErrorStr, binPersonStr);

//        final boolean includeSensitive = getBoolean(IGNITE_TO_STRING_INCLUDE_SENSITIVE);

        if (S.getSensitiveDataLogging() == PLAIN) {
            assertContains(log, strFailedSndStr, binKeyStr);
            assertContains(log, strFailedSndStr, binPersonStr);
            assertContains(log, strReceivedErrorStr, binKeyStr);
            assertContains(log, strReceivedErrorStr, binPersonStr);

//            assertContains(log, maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binKey, Key.class)));
//
//            assertContains(log, maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binPerson, Person.class)));
        }
        else {
            Pattern patternKey;
            Pattern patternVal;
            
            if (S.getSensitiveDataLogging() == HASH) {
                patternKey = Pattern.compile("(IgniteTxKey \\[key=" + binKey.hashCode() + ", cacheId=\\d+\\])");
                patternVal = Pattern.compile("(TxEntryValueHolder \\[val=" + binPerson.hashCode() + ", op=\\D+\\])");
            }
            else {
                patternKey = Pattern.compile("(IgniteTxKey \\[cacheId=\\d+\\])");
                patternVal = Pattern.compile("(TxEntryValueHolder \\[op=\\D+\\])");
            }
            final Matcher matcherKeySnd = patternKey.matcher(strFailedSndStr);
            final Matcher matcherKeyReceived = patternKey.matcher(strReceivedErrorStr);

            assertTrue(matcherKeySnd.find());
            assertTrue(matcherKeyReceived.find());

            final Matcher matcherValSnd = patternVal.matcher(strFailedSndStr);
            final Matcher matcherValReceived = patternVal.matcher(strReceivedErrorStr);

            assertTrue(matcherValSnd.find());
            assertTrue(matcherValReceived.find());
            
//            assertNotContains(log, strFailedSndStr, binPersonStr);
//            assertNotContains(log, strReceivedErrorStr, binPersonStr);
        
    }
    }

    /**
     * Removes a idHash from a string.
     *
     * @param s String.
     * @return String without a idHash.
     */
    private String maskIdHash(String s) {
        assert nonNull(s);

        return s.replaceAll("idHash=[^,]*", "idHash=NO");
    }

    /**
     * Create a string to search for BinaryObject in the log.
     *
     * @param binPerson BinaryObject.
     * @param cls Class of BinaryObject.
     * @return String representation of BinaryObject.
     */
    private String toStr(BinaryObject binPerson, Class<?> cls) {
        assert nonNull(binPerson);
        assert nonNull(cls);

        return binPerson.toString().replace(cls.getName(), cls.getSimpleName());
    }

    /**
     * Key for mapping value in cache.
     */
    static class Key {
        /** Id. */
        int id;

        /**
         * Constructor.
         *
         * @param id Id.
         */
        public Key(int id) {
            this.id = id;
        }
    }

    /**
     * Person class for cache storage.
     */
    static class Person {
        /** Id organization. */
        int orgId;

        /** Person name. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Id organization.
         * @param name Person name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
