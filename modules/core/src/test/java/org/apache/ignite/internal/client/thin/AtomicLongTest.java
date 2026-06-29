/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientAtomicConfiguration;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests client atomic long.
 * Partition awareness tests are in {@link ThinClientAffinityAwarenessStableTopologyTest#testAtomicLong()}.
 */
public class AtomicLongTest extends AbstractThinClientTest {
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
            ClientAtomicLong atomicLong = client.atomicLong(name, 42, true);

            ClientAtomicLong atomicLongWithGroup = client.atomicLong(
                    name, new ClientAtomicConfiguration().setGroupName("grp"), 43, true);

            assertEquals(42, atomicLong.get());
            assertEquals(43, atomicLongWithGroup.get());
        }
    }

    /**
     * Tests initial value setting (async factory).
     */
    @Test
    public void testAsyncCreateSetsInitialValue() throws Exception {
        String name = "testAsyncCreateSetsInitialValue";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLongAsync(name, 42, true).get();

            ClientAtomicLong atomicLongWithGroup = client.atomicLongAsync(
                    name, new ClientAtomicConfiguration().setGroupName("grp"), 43, true).get();

            assertEquals(42, atomicLong.get());
            assertEquals(43, atomicLongWithGroup.get());
        }
    }

    /**
     * Tests that initial value is ignored when atomic long already exists.
     */
    @Test
    public void testCreateIgnoresInitialValueWhenAlreadyExists() {
        String name = "testCreateIgnoresInitialValueWhenAlreadyExists";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 42, true);
            ClientAtomicLong atomicLong2 = client.atomicLong(name, -42, true);

            assertEquals(42, atomicLong.get());
            assertEquals(42, atomicLong2.get());
        }
    }

    /**
     * Tests that exception is thrown when atomic long does not exist.
     */
    @Test
    public void testOperationsThrowExceptionWhenAtomicLongDoesNotExist() {
        try (IgniteClient client = startClient(0)) {
            String name = "testOperationsThrowExceptionWhenAtomicLongDoesNotExist";
            ClientAtomicLong atomicLong = client.atomicLong(name, 0, true);
            atomicLong.close();

            assertDoesNotExistError(name, atomicLong::get);

            assertDoesNotExistError(name, atomicLong::incrementAndGet);
            assertDoesNotExistError(name, atomicLong::getAndIncrement);
            assertDoesNotExistError(name, atomicLong::decrementAndGet);
            assertDoesNotExistError(name, atomicLong::getAndDecrement);

            assertDoesNotExistError(name, () -> atomicLong.addAndGet(1));
            assertDoesNotExistError(name, () -> atomicLong.getAndAdd(1));

            assertDoesNotExistError(name, () -> atomicLong.getAndSet(1));
            assertDoesNotExistError(name, () -> atomicLong.compareAndSet(1, 2));
        }
    }

    /**
     * Tests that async operations complete exceptionally when the atomic long does not exist.
     */
    @Test
    public void testAsyncOperationsThrowExceptionWhenAtomicLongDoesNotExist() throws Exception {
        String name = "testAsyncOperationsThrowExceptionWhenAtomicLongDoesNotExist";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 0, true);
            atomicLong.close();

            assertAsyncDoesNotExistError(name, atomicLong::getAsync);

            assertAsyncDoesNotExistError(name, atomicLong::incrementAndGetAsync);
            assertAsyncDoesNotExistError(name, atomicLong::getAndIncrementAsync);
            assertAsyncDoesNotExistError(name, atomicLong::decrementAndGetAsync);
            assertAsyncDoesNotExistError(name, atomicLong::getAndDecrementAsync);

            assertAsyncDoesNotExistError(name, () -> atomicLong.addAndGetAsync(1));
            assertAsyncDoesNotExistError(name, () -> atomicLong.getAndAddAsync(1));

            assertAsyncDoesNotExistError(name, () -> atomicLong.getAndSetAsync(1));
            assertAsyncDoesNotExistError(name, () -> atomicLong.compareAndSetAsync(1, 2));
        }
    }

    /**
     * Tests that initial value is ignored when atomic long already exists (async factory).
     */
    @Test
    public void testAsyncCreateIgnoresInitialValueWhenAlreadyExists() throws Exception {
        String name = "testAsyncCreateIgnoresInitialValueWhenAlreadyExists";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLongAsync(name, 42, true).get();
            ClientAtomicLong atomicLong2 = client.atomicLongAsync(name, -42, true).get();

            assertEquals(42, atomicLong.get());
            assertEquals(42, atomicLong2.get());
        }
    }

    /**
     * Tests removed property.
     */
    @Test
    public void testRemoved() {
        String name = "testRemoved";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 0, false);
            assertNull(atomicLong);

            atomicLong = client.atomicLong(name, 1, true);
            assertFalse(atomicLong.removed());
            assertEquals(1, atomicLong.get());

            atomicLong.close();
            assertTrue(atomicLong.removed());
        }
    }

    /**
     * Tests removed property (async factory).
     */
    @Test
    public void testAsyncRemoved() throws Exception {
        String name = "testAsyncRemoved";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLongAsync(name, 0, false).get();
            assertNull(atomicLong);

            atomicLong = client.atomicLongAsync(name, 1, true).get();
            assertFalse(atomicLong.removedAsync().get());
            assertEquals(1, atomicLong.get());

            atomicLong.close();
            assertTrue(atomicLong.removedAsync().get());
        }
    }

    /**
     * Tests increment, decrement, add.
     */
    @Test
    public void testIncrementDecrementAdd() {
        String name = "testIncrementDecrementAdd";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertEquals(2, atomicLong.incrementAndGet());
            assertEquals(2, atomicLong.getAndIncrement());

            assertEquals(3, atomicLong.get());

            assertEquals(2, atomicLong.decrementAndGet());
            assertEquals(2, atomicLong.getAndDecrement());

            assertEquals(1, atomicLong.get());

            assertEquals(101, atomicLong.addAndGet(100));
            assertEquals(101, atomicLong.getAndAdd(-50));

            assertEquals(51, atomicLong.get());
        }
    }

    /**
     * Tests async increment, decrement, and add operations.
     */
    @Test
    public void testIncrementDecrementAddAsync() throws Exception {
        String name = "testIncrementDecrementAddAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong a = client.atomicLong(name, 1, true);

            assertEquals(2L, (long) a.incrementAndGetAsync().get());
            assertEquals(2L, (long) a.getAndIncrementAsync().get()); // returns old (2), increments to 3

            assertEquals(3L, a.get());

            assertEquals(2L, (long) a.decrementAndGetAsync().get());
            assertEquals(2L, (long) a.getAndDecrementAsync().get()); // returns old (2), decrements to 1

            assertEquals(1L, a.get());

            assertEquals(101L, (long) a.addAndGetAsync(100).get());
            assertEquals(101L, (long) a.getAndAddAsync(-50).get()); // returns old (101), adds -50 to get 51

            assertEquals(51L, a.get());
        }
    }

    /**
     * Tests getAndSet.
     */
    @Test
    public void testGetAndSet() {
        String name = "testGetAndSet";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertEquals(1, atomicLong.getAndSet(100));
            assertEquals(100, atomicLong.get());
        }
    }

    /**
     * Tests {@link ClientAtomicLong#getAndSetAsync(long)}.
     */
    @Test
    public void testGetAndSetAsync() throws Exception {
        String name = "testGetAndSetAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertEquals(1L, (long) atomicLong.getAndSetAsync(100).get());
            assertEquals(100L, atomicLong.get());
        }
    }

    /**
     * Tests compareAndSet.
     */
    @Test
    public void testCompareAndSet() {
        String name = "testCompareAndSet";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertFalse(atomicLong.compareAndSet(2, 3));
            assertEquals(1, atomicLong.get());

            assertTrue(atomicLong.compareAndSet(1, 4));
            assertEquals(4, atomicLong.get());
        }
    }

    /**
     * Tests {@link ClientAtomicLong#compareAndSetAsync(long, long)}.
     */
    @Test
    public void testCompareAndSetAsync() throws Exception {
        String name = "testCompareAndSetAsync";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicLong = client.atomicLong(name, 1, true);

            assertFalse(atomicLong.compareAndSetAsync(2, 3).get());
            assertEquals(1L, atomicLong.get());

            assertTrue(atomicLong.compareAndSetAsync(1, 4).get());
            assertEquals(4L, atomicLong.get());
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
            client.atomicLong(name, cfg1, 1, true);
            client.atomicLong(name, cfg2, 2, true);
            client.atomicLong(name, 3, true);
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

    /**
     * Tests atomic long with custom configuration (async factory).
     */
    @Test
    public void testAsyncCustomConfigurationPropagatesToServer() throws Exception {
        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(64)
                .setBackups(2)
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName("async-al-group-partitioned");

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setAtomicSequenceReserveSize(32)
                .setBackups(3)
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName("async-al-group-replicated");

        String name = "testAsyncCustomConfiguration";

        try (IgniteClient client = startClient(0)) {
            client.atomicLongAsync(name, cfg1, 1, true).get();
            client.atomicLongAsync(name, cfg2, 2, true).get();
        }

        IgniteInternalCache<?, ?> partitionedCache = grid(0).cachesx().stream()
                .filter(c -> c.name().equals("ignite-sys-atomic-cache@async-al-group-partitioned"))
                .findFirst().orElse(null);

        IgniteInternalCache<?, ?> replicatedCache = grid(0).cachesx().stream()
                .filter(c -> c.name().equals("ignite-sys-atomic-cache@async-al-group-replicated"))
                .findFirst().orElse(null);

        assertNotNull(partitionedCache);
        assertNotNull(replicatedCache);

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
            ClientAtomicLong al1 = client.atomicLong(name, cfg1, 1, true);
            ClientAtomicLong al2 = client.atomicLong(name, cfg2, 2, true);
            ClientAtomicLong al3 = client.atomicLong(name, 3, true);

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

    /**
     * Tests atomic long with same name and group name, but different cache modes (async factory).
     */
    @Test
    public void testAsyncSameNameDifferentOptionsDoesNotCreateSecondAtomic() throws Exception {
        String groupName = "testAsyncSameNameDifferentOptions";

        ClientAtomicConfiguration cfg1 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.REPLICATED)
                .setGroupName(groupName);

        ClientAtomicConfiguration cfg2 = new ClientAtomicConfiguration()
                .setCacheMode(CacheMode.PARTITIONED)
                .setGroupName(groupName);

        String name = "testAsyncSameNameDifferentOptionsDoesNotCreateSecondAtomic";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong al1 = client.atomicLongAsync(name, cfg1, 1, true).get();
            ClientAtomicLong al2 = client.atomicLongAsync(name, cfg2, 2, true).get();
            ClientAtomicLong al3 = client.atomicLongAsync(name, 3, true).get();

            assertEquals(1, al1.get());
            assertEquals(1, al2.get());
            assertEquals(3, al3.get());
        }

        List<IgniteInternalCache<?, ?>> caches = grid(0).cachesx().stream()
                .filter(c -> c.name().contains(groupName))
                .collect(Collectors.toList());

        assertEquals(1, caches.size());

        IgniteInternalCache<?, ?> replicatedCache = caches.get(0);

        assertEquals("ignite-sys-atomic-cache@testAsyncSameNameDifferentOptions", replicatedCache.name());
        assertEquals(Integer.MAX_VALUE, replicatedCache.configuration().getBackups());
    }

    @Test
    public void testToString() {
        String name = "testToString";

        try (IgniteClient client = startClient(0)) {
            ClientAtomicLong atomicSequence = client.atomicLong(name, 0, true);

            assertEquals(
                    "ClientAtomicLongImpl [super=" +
                            "AbstractClientAtomic [name=testToString, groupName=null, cacheId=1481046058]]",
                    atomicSequence.toString());
        }
    }

    /**
     * Asserts that "does not exist" error is thrown.
     *
     * @param name Atomic long name.
     * @param callable Callable.
     */
    private void assertDoesNotExistError(String name, Callable<Object> callable) {
        ClientException ex = assertThrows(null, callable, ClientException.class, null);

        assertContains(null, ex.getMessage(), "AtomicLong with name '" + name + "' does not exist.");
        assertEquals(ClientStatus.RESOURCE_DOES_NOT_EXIST, ((ClientServerError)ex.getCause()).getCode());
    }

    /**
     * Asserts that "does not exist" error is the cause of the {@link ExecutionException} thrown by an async operation.
     *
     * @param name Atomic long name.
     * @param futureCallable Callable returning an {@link IgniteClientFuture}.
     */
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
}
