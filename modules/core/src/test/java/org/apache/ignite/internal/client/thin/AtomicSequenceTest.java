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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientAtomicConfiguration;
import org.apache.ignite.client.ClientAtomicSequence;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests client atomic sequence.
 */
@SuppressWarnings("resource")
public class AtomicSequenceTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration() {
        return super.getClientConfiguration().setAffinityAwarenessEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Tests initial value setting.
     */
    @Test
    public void testCreateSetsInitialValue() {
        String name = "testCreateSetsInitialValue";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence atomicSequence = client.atomicSequence(name, 42, true);

            ClientAtomicSequence atomicSequenceWithGroup = client.atomicSequence(
                    name, new ClientAtomicConfiguration().setGroupName("grp"), 43, true);

            assertEquals(42, atomicSequence.get());
            assertEquals(43, atomicSequenceWithGroup.get());
        }
    }

    @Test
    public void testCreateSetsInitialValueAsync() throws Exception {
        String name = "testCreateSetsInitialValueAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence atomicSequence = client.atomicSequenceAsync(name, 42, true).get();

            ClientAtomicSequence atomicSequenceWithGroup = client.atomicSequenceAsync(
                    name, new ClientAtomicConfiguration().setGroupName("grp"), 43, true).get();

            assertEquals(42L, atomicSequence.get());
            assertEquals(43L, atomicSequenceWithGroup.get());
        }
    }

    /**
     * Tests initial value setting.
     */
    @Test
    public void testNegativeInitialValue() {
        String name = "testNegativeInitialValue";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence seq = client.atomicSequence(name, -100, true);

            assertEquals(-100, seq.get());
            assertEquals(-99, seq.incrementAndGet());
            assertEquals(-99, seq.getAndIncrement());
            assertEquals(-97, seq.incrementAndGet());
        }
    }

    @Test
    public void testNegativeInitialValueAsync() throws Exception {
        String name = "testNegativeInitialValueAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence seq = client.atomicSequenceAsync(name, -100, true).get();

            assertEquals(-100L, seq.get());
            assertEquals(-99L, (long) seq.incrementAndGetAsync().get());
            assertEquals(-99L, (long) seq.getAndIncrementAsync().get());
            assertEquals(-97L, (long) seq.incrementAndGetAsync().get());
        }
    }

    /**
     * Tests that initial value is ignored when atomic long already exists.
     */
    @Test
    public void testCreateIgnoresInitialValueWhenAlreadyExists() {
        String name = "testCreateIgnoresInitialValueWhenAlreadyExists";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence atomicSeq = client.atomicSequence(name, 42, true);
            ClientAtomicSequence atomicSeq2 = client.atomicSequence(name, -42, true);

            assertEquals(42, atomicSeq.get());
            assertEquals(42, atomicSeq2.get());
        }
    }

    @Test
    public void testCreateIgnoresInitialValueWhenAlreadyExistsAsync() throws Exception {
        String name = "testCreateIgnoresInitialValueWhenAlreadyExistsAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence atomicSeq = client.atomicSequenceAsync(name, 42, true).get();
            ClientAtomicSequence atomicSeq2 = client.atomicSequenceAsync(name, -42, true).get();

            assertEquals(42L, atomicSeq.get());
            assertEquals(42L, atomicSeq2.get());
        }
    }

    /**
     * Tests that exception is thrown when atomic long does not exist.
     */
    @Test
    public void testOperationsThrowExceptionWhenAtomicSequenceDoesNotExist() {
        try (IgniteClient client = startClient(0)) {
            String name = "testOperationsThrowExceptionWhenAtomicSequenceDoesNotExist";

            ClientAtomicSequence atomicSequence = client.atomicSequence(name, 0, true);
            atomicSequence.batchSize(1);
            atomicSequence.close();

            assertDoesNotExistError(name, atomicSequence::get);

            assertDoesNotExistError(name, atomicSequence::incrementAndGet);
            assertDoesNotExistError(name, atomicSequence::getAndIncrement);

            assertDoesNotExistError(name, () -> atomicSequence.addAndGet(1));
            assertDoesNotExistError(name, () -> atomicSequence.getAndAdd(1));
        }
    }

    /**
     * Tests that async operations throw when the sequence does not exist.
     * The exception is thrown synchronously since {@code checkRemoved()} runs before the future is created.
     */
    @Test
    public void testAsyncOperationsThrowExceptionWhenAtomicSequenceDoesNotExist() throws Exception {
        try (IgniteClient client = startClient(0)) {
            String name = "testAsyncOperationsThrowExceptionWhenAtomicSequenceDoesNotExist";

            ClientAtomicSequence atomicSequence = client.atomicSequenceAsync(name, 0, true).get();
            atomicSequence.batchSize(1);
            atomicSequence.close();

            assertAsyncDoesNotExistError(name, atomicSequence::incrementAndGetAsync);
            assertAsyncDoesNotExistError(name, atomicSequence::getAndIncrementAsync);

            assertAsyncDoesNotExistError(name, () -> atomicSequence.addAndGetAsync(1));
            assertAsyncDoesNotExistError(name, () -> atomicSequence.getAndAddAsync(1));
        }
    }

    /**
     * Tests removed property.
     */
    @Test
    public void testRemoved() {
        String name = "testRemoved";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence seq = client.atomicSequence(name, 0, false);
            assertNull(seq);

            seq = client.atomicSequence(name, 1, true);
            ClientAtomicSequence seq2 = client.atomicSequence(name, 0, false);

            assertFalse(seq.removed());
            assertFalse(seq2.removed());

            assertEquals(1, seq.get());
            assertEquals(1, seq2.get());

            seq.close();
            assertTrue(seq.removed());
            assertTrue(seq2.removed());
        }
    }

    @Test
    public void testRemovedAsync() throws Exception {
        String name = "testRemovedAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence seq = client.atomicSequenceAsync(name, 0, false).get();
            assertNull(seq);

            seq = client.atomicSequenceAsync(name, 1, true).get();
            ClientAtomicSequence seq2 = client.atomicSequenceAsync(name, 0, false).get();

            assertFalse(seq.removed());
            assertFalse(seq2.removed());

            assertEquals(1L, seq.get());
            assertEquals(1L, seq2.get());

            seq.close();
            assertTrue(seq.removed());
            assertTrue(seq2.removed());
        }
    }

    /**
     * Tests increment, add.
     */
    @Test
    public void testIncrementAdd() {
        String name = "testIncrementAdd";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence atomicSequence = client.atomicSequence(name, 1, true);

            assertEquals(2, atomicSequence.incrementAndGet());
            assertEquals(2, atomicSequence.getAndIncrement());

            assertEquals(3, atomicSequence.get());

            assertEquals(103, atomicSequence.addAndGet(100));
            assertEquals(103, atomicSequence.getAndAdd(1000));

            assertEquals(1103, atomicSequence.get());
        }
    }

    /**
     * Tests async increment, add.
     */
    @Test
    public void testIncrementAddAsync() throws Exception {
        String name = "testIncrementAddAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence seq = client.atomicSequenceAsync(name, 1, true).get();

            assertEquals(2L, (long) seq.incrementAndGetAsync().get());
            assertEquals(2L, (long) seq.getAndIncrementAsync().get());

            assertEquals(3L, seq.get());

            assertEquals(103L, (long) seq.addAndGetAsync(100).get());
            assertEquals(103L, (long) seq.getAndAddAsync(1000).get());

            assertEquals(1103L, seq.get());
        }
    }

    /**
     * Tests atomic long with custom configuration.
     */
    @Test
    public void testCustomConfigurationPropagatesToServer() {
        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(64)
                .setBackups(2)
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName("atomic-long-group-partitioned");

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(32)
                .setBackups(3)
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName("atomic-long-group-replicated");

        String name = "testCustomConfiguration";

        try (IgniteClient client = startClient(0)) {
            client.atomicSequence(name, cfg1, 1, true);
            client.atomicSequence(name, cfg2, 2, true);
            client.atomicSequence(name, 3, true);
        }

        List<IgniteInternalCache<?, ?>> caches = new ArrayList<>(grid(0).cachesx());
        assertEquals(4, caches.size());

        IgniteInternalCache<?, ?> partitionedCache = caches.get(1);
        IgniteInternalCache<?, ?> replicatedCache = caches.get(2);
        IgniteInternalCache<?, ?> defaultCache = caches.get(3);

        assertEquals("ignite-sys-atomic-cache@atomic-long-group-partitioned", partitionedCache.name());
        assertEquals("ignite-sys-atomic-cache@atomic-long-group-replicated", replicatedCache.name());
        assertEquals("ignite-sys-atomic-cache@default-ds-group", defaultCache.name());

        assertEquals(2, partitionedCache.configuration().getBackups());
        assertEquals(Integer.MAX_VALUE, replicatedCache.configuration().getBackups());
        assertEquals(1, defaultCache.configuration().getBackups());
    }

    @Test
    public void testCustomConfigurationPropagatesToServerAsync() throws Exception {
        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(64)
                .setBackups(2)
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName("atomic-seq-group-partitioned-async");

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(32)
                .setBackups(3)
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName("atomic-seq-group-replicated-async");

        String name = "testCustomConfigurationAsync";

        try (IgniteClient client = startClient(0)) {
            CompletableFuture.allOf(
                    client.atomicSequenceAsync(name, cfg1, 1, true).toCompletableFuture(),
                    client.atomicSequenceAsync(name, cfg2, 2, true).toCompletableFuture()
            ).get();
        }

        IgniteInternalCache<?, ?> partitionedCache = grid(0).cachesx().stream()
                .filter(c -> c.name().contains("atomic-seq-group-partitioned-async"))
                .findFirst().orElseThrow(() -> new AssertionError("Partitioned cache not found"));

        IgniteInternalCache<?, ?> replicatedCache = grid(0).cachesx().stream()
                .filter(c -> c.name().contains("atomic-seq-group-replicated-async"))
                .findFirst().orElseThrow(() -> new AssertionError("Replicated cache not found"));

        assertEquals("ignite-sys-atomic-cache@atomic-seq-group-partitioned-async", partitionedCache.name());
        assertEquals("ignite-sys-atomic-cache@atomic-seq-group-replicated-async", replicatedCache.name());

        assertEquals(2, partitionedCache.configuration().getBackups());
        assertEquals(Integer.MAX_VALUE, replicatedCache.configuration().getBackups());
    }

    /**
     * Tests atomic long with same name and group name, but different cache modes.
     */
    @Test
    public void testSameNameDifferentOptionsDoesNotCreateSecondAtomic() {
        String groupName = "testSameNameDifferentOptions";

        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName(groupName);

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName(groupName);

        String name = "testSameNameDifferentOptionsDoesNotCreateSecondAtomic";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence al1 = client.atomicSequence(name, cfg1, 1, true);
            ClientAtomicSequence al2 = client.atomicSequence(name, cfg2, 2, true);
            ClientAtomicSequence al3 = client.atomicSequence(name, 3, true);

            assertEquals(1, al1.get());
            assertEquals(1, al2.get());
            assertEquals(3, al3.get());
        }

        List<IgniteInternalCache<?, ?>> caches = grid(0).cachesx().stream()
                .filter(c -> c.name().contains(groupName))
                .collect(Collectors.toList());

        assertEquals(1, caches.size());

        IgniteInternalCache<?, ?> replicatedCache = caches.get(0);

        assertEquals("ignite-sys-atomic-cache@testSameNameDifferentOptions", replicatedCache.name());
        assertEquals(Integer.MAX_VALUE, replicatedCache.configuration().getBackups());
    }

    @Test
    public void testSameNameDifferentOptionsDoesNotCreateSecondAtomicAsync() throws Exception {
        String groupName = "asyncDifferentOptionsSameName";

        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName(groupName);

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName(groupName);

        String name = "testSameNameDifferentOptionsDoesNotCreateSecondAtomicAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence al1 = client.atomicSequenceAsync(name, cfg1, 1, true).get();
            ClientAtomicSequence al2 = client.atomicSequenceAsync(name, cfg2, 2, true).get();
            ClientAtomicSequence al3 = client.atomicSequenceAsync(name, 3, true).get();

            assertEquals(1L, al1.get());
            assertEquals(1L, al2.get());
            assertEquals(3L, al3.get());
        }

        List<IgniteInternalCache<?, ?>> caches = grid(0).cachesx().stream()
                .filter(c -> c.name().contains(groupName))
                .collect(Collectors.toList());

        assertEquals(1, caches.size());

        IgniteInternalCache<?, ?> replicatedCache = caches.get(0);

        assertEquals("ignite-sys-atomic-cache@asyncDifferentOptionsSameName", replicatedCache.name());
        assertEquals(Integer.MAX_VALUE, replicatedCache.configuration().getBackups());
    }

    @Test
    public void testIncrementIntegrity() {
        String seqName = UUID.randomUUID().toString();

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence locSeq = client.atomicSequence(seqName, 0, true);
            locSeq.batchSize(3);

            for (int i = 0; i < 100; i++) {
                assertEquals(i + 1, locSeq.incrementAndGet());
            }
        }
    }

    @Test
    public void testIncrementIntegrityAsync() throws Exception {
        String seqName = UUID.randomUUID().toString();

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence locSeq = client.atomicSequenceAsync(seqName, 0, true).get();
            locSeq.batchSize(3);

            for (int i = 0; i < 100; i++) {
                assertEquals((long) (i + 1), (long) locSeq.incrementAndGetAsync().get());
            }
        }
    }

    @Test
    public void testAddIntegrity() {
        String seqName = UUID.randomUUID().toString();

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence locSeq = client.atomicSequence(seqName, 0, true);
            locSeq.batchSize(3);

            for (int i = 0; i < 100; i++) {
                assertEquals((i + 1) * 2, locSeq.addAndGet(2));
            }
        }
    }

    @Test
    public void testAddIntegrityAsync() throws Exception {
        String seqName = UUID.randomUUID().toString();

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence locSeq = client.atomicSequenceAsync(seqName, 0, true).get();
            locSeq.batchSize(3);

            for (int i = 0; i < 100; i++) {
                assertEquals((long) (i + 1) * 2, (long) locSeq.addAndGetAsync(2).get());
            }
        }
    }

    @Test
    public void testIncrementIntegrityTwoInstances() {
        String seqName = "testSequenceIntegrityTwoInstances";
        int count = 100;

        try (IgniteClient client = startClient(0)) {
            // Two instances with the same name and different batch sizes should produce correct sequence
            // without gaps and duplicates.
            ClientAtomicSequence locSeq = client.atomicSequence(seqName, 0, true);
            ClientAtomicSequence locSeq2 = client.atomicSequence(seqName, 0, true);

            locSeq.batchSize(3);
            locSeq2.batchSize(4);

            Set<Long> expected = LongStream.rangeClosed(1, count * 2).boxed().collect(Collectors.toSet());

            for (int i = 0; i < count; i++) {
                long val1 = locSeq.incrementAndGet();
                long val2 = locSeq2.incrementAndGet();

                assertTrue("val1: " + val1, expected.remove(val1));
                assertTrue("val2: " + val2, expected.remove(val2));
            }

            assertTrue(expected.isEmpty());
        }
    }

    @Test
    public void testIncrementIntegrityTwoInstancesAsync() throws Exception {
        String seqName = "testSequenceIntegrityTwoInstancesAsync";
        int count = 100;

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence locSeq = client.atomicSequenceAsync(seqName, 0, true).get();
            ClientAtomicSequence locSeq2 = client.atomicSequenceAsync(seqName, 0, true).get();

            locSeq.batchSize(3);
            locSeq2.batchSize(4);

            Set<Long> expected = LongStream.rangeClosed(1, count * 2).boxed().collect(Collectors.toSet());

            for (int i = 0; i < count; i++) {
                IgniteClientFuture<Long> fut1 = locSeq.incrementAndGetAsync();
                IgniteClientFuture<Long> fut2 = locSeq2.incrementAndGetAsync();

                long val1 = fut1.get();
                long val2 = fut2.get();

                assertTrue("val1: " + val1, expected.remove(val1));
                assertTrue("val2: " + val2, expected.remove(val2));
            }

            assertTrue(expected.isEmpty());
        }
    }

    @Test
    public void testReservedRange() {
        String seqName = "testReservedRange";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence seq = client.atomicSequence(seqName, 33, true);

            seq.batchSize(3);
            assertEquals(3, seq.batchSize());

            assertEquals(33, seq.get());
            assertEquals(33, getRemoteValue(client, seqName));

            assertEquals(34, seq.incrementAndGet());
            assertEquals(37, getRemoteValue(client, seqName));

            assertEquals(35, seq.incrementAndGet());
            assertEquals(37, getRemoteValue(client, seqName));

            assertEquals(36, seq.incrementAndGet());
            assertEquals(37, getRemoteValue(client, seqName));

            assertEquals(37, seq.incrementAndGet());
            assertEquals(41, getRemoteValue(client, seqName));

            assertEquals(38, seq.incrementAndGet());
            assertEquals(41, getRemoteValue(client, seqName));

            assertEquals(39, seq.incrementAndGet());
            assertEquals(41, getRemoteValue(client, seqName));

            assertEquals(40, seq.incrementAndGet());
            assertEquals(41, getRemoteValue(client, seqName));

            seq.batchSize(10);

            assertEquals(41, seq.incrementAndGet());
            assertEquals(52, getRemoteValue(client, seqName));
        }
    }

    @Test
    public void testReservedRangeAsync() throws Exception {
        String seqName = "testReservedRangeAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence seq = client.atomicSequenceAsync(seqName, 33, true).get();

            seq.batchSize(3);
            assertEquals(3, seq.batchSize());

            assertEquals(33L, seq.get());
            assertEquals(33L, getRemoteValue(client, seqName));

            assertEquals(34L, (long) seq.incrementAndGetAsync().get());
            assertEquals(37L, getRemoteValue(client, seqName));

            assertEquals(35L, (long) seq.incrementAndGetAsync().get());
            assertEquals(37L, getRemoteValue(client, seqName));

            assertEquals(36L, (long) seq.incrementAndGetAsync().get());
            assertEquals(37L, getRemoteValue(client, seqName));

            assertEquals(37L, (long) seq.incrementAndGetAsync().get());
            assertEquals(41L, getRemoteValue(client, seqName));

            assertEquals(38L, (long) seq.incrementAndGetAsync().get());
            assertEquals(41L, getRemoteValue(client, seqName));

            assertEquals(39L, (long) seq.incrementAndGetAsync().get());
            assertEquals(41L, getRemoteValue(client, seqName));

            assertEquals(40L, (long) seq.incrementAndGetAsync().get());
            assertEquals(41L, getRemoteValue(client, seqName));

            seq.batchSize(10);

            assertEquals(41L, (long) seq.incrementAndGetAsync().get());
            assertEquals(52L, getRemoteValue(client, seqName));
        }
    }

    @Test
    public void testToString() {
        String name = "testToString";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicSequence atomicSequence = client.atomicSequence(name, 0, true);

            assertEquals(
                    "ClientAtomicSequenceImpl [batchSize=1000, rmvd=false, super=" +
                            "AbstractClientAtomic [name=testToString, groupName=null, cacheId=1481046058]]",
                    atomicSequence.toString());
        }
    }

    private void assertDoesNotExistError(String name, Callable<Object> callable) {
        IgniteException ex = assertThrows(null, callable, IgniteException.class, null);

        assertContains(null, ex.getMessage(), "Sequence was removed from cache: " + name);
    }

    private void assertAsyncDoesNotExistError(String name, Callable<? extends IgniteClientFuture<?>> futureCallable) {
        assertDoesNotExistError(name, () -> {
            try {
                return futureCallable.call().get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                throw cause instanceof RuntimeException ? (RuntimeException) cause : new RuntimeException(e);
            }
        });
    }

    private static long getRemoteValue(IgniteClient client, String seqName) {
        return client.atomicSequence(seqName, 0, false).get();
    }
}
