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

package org.apache.ignite.internal.processors.cache.checker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ConsistencyCheckUtilsTest {
    /** Node 2. */
    private static final UUID NODE_2 = UUID.randomUUID();

    /** Node 3. */
    private static final UUID NODE_3 = UUID.randomUUID();

    /** Node 4. */
    private static final UUID NODE_4 = UUID.randomUUID();

    /** Node 1. */
    private static final UUID NODE_1 = UUID.randomUUID();

    /** Test key. */
    private static final KeyCacheObject TEST_KEY = new KeyCacheObjectImpl(123, null, -1);

    /** Cctx. */
    private GridCacheContext cctx = Mockito.mock(GridCacheContext.class);

    /** Topology. */
    private GridDhtPartitionTopology top = Mockito.mock(GridDhtPartitionTopology.class);

    /** Aff. */
    private GridCacheAffinityManager aff = Mockito.mock(GridCacheAffinityManager.class);

    /** Owners. */
    private List owners = Mockito.mock(List.class);

    {
        when(cctx.topology()).thenReturn(top);

        when(top.owners(anyInt(), any())).thenReturn(owners);

        when(cctx.affinity()).thenReturn(aff);

        when(aff.partition(any())).thenReturn(1);
    }

    /**
     *
     */
    @Test
    public void testCheckConsistency() {
        Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
        oldKey.put(NODE_1, version(1));
        oldKey.put(NODE_2, version(3));
        oldKey.put(NODE_3, version(3));
        oldKey.put(NODE_4, version(2));

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>(); // All keys was removed

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(1));
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(4)); // Max version increase
            actualKey.put(NODE_4, versionedValue(2));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(1));
            actualKey.put(NODE_2, versionedValue(3)); // Max of node 3 was removed
            actualKey.put(NODE_4, versionedValue(2));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(3)); // Min value like max
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(3));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(4)); // Min value greater then max
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(1)); // Nothing changed.
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(2));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(2)); // Not all min values were incremented.
            actualKey.put(NODE_2, versionedValue(3));
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(3));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_2, versionedValue(3)); // Remove of one value is not enough
            actualKey.put(NODE_3, versionedValue(3));
            actualKey.put(NODE_4, versionedValue(3));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 4));
        }
    }

    /**
     *
     */
    @Test
    public void testCheckConsistencyMaxGroup() {
        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(NODE_1, version(1));
            oldKey.put(NODE_2, version(1));

            {
                Map<UUID, VersionedValue> actualKey = new HashMap<>();
                actualKey.put(NODE_1, versionedValue(1));

                assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
            }

            {
                Map<UUID, VersionedValue> actualKey = new HashMap<>();
                actualKey.put(NODE_2, versionedValue(1));

                assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
            }
        }

        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(NODE_1, version(1));
            oldKey.put(NODE_2, version(2));
            oldKey.put(NODE_3, version(3));

            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_3, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
        }

        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(NODE_1, version(3));
            oldKey.put(NODE_2, version(2));
            oldKey.put(NODE_3, version(1));

            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(NODE_1, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey, 3));
        }
    }

    /**
     * If a max element missing in set of old keys, we must swap old to actual keys.
     */
    @Test
    public void testOldKeySizeLessThenOwnerAndMaxElementIsMissing() {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> oldKeys = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeyVers = new HashMap<>();
        oldKeyVers.put(NODE_1, version(3));
        oldKeyVers.put(NODE_2, version(2));
        oldKeyVers.put(NODE_3, version(1));
        oldKeys.put(TEST_KEY, oldKeyVers);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = new HashMap<>();
        Map<UUID, VersionedValue> actualKeyVers = new HashMap<>();
        actualKeyVers.put(NODE_4, versionedValue(4));
        actualKeyVers.put(NODE_1, versionedValue(3));
        actualKeyVers.put(NODE_2, versionedValue(2));
        actualKeyVers.put(NODE_3, versionedValue(1));
        actualKeys.put(TEST_KEY, actualKeyVers);

        when(owners.size()).thenReturn(4);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckNeeded
            = ConsistencyCheckUtils.checkConflicts(oldKeys, actualKeys, cctx, null);

        assertEquals(1, recheckNeeded.size());
        assertEquals(recheckNeeded.get(TEST_KEY).size(), 4);
        assertEquals(recheckNeeded.get(TEST_KEY).get(NODE_4), version(4));
    }

    /**
     *
     */
    @Test
    public void testOldKeySizeLessThenOwnerAndAnyOldKeyIsMissingInActualKeysCheckSuccess() {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> oldKeys = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeyVers = new HashMap<>();
        oldKeyVers.put(NODE_1, version(3));
        oldKeyVers.put(NODE_2, version(2));
        oldKeyVers.put(NODE_3, version(1));
        oldKeys.put(TEST_KEY, oldKeyVers);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = new HashMap<>();
        Map<UUID, VersionedValue> actualKeyVers = new HashMap<>();
        actualKeyVers.put(NODE_4, versionedValue(4));
        actualKeyVers.put(NODE_1, versionedValue(3));
        actualKeyVers.put(NODE_2, versionedValue(2));
        actualKeys.put(TEST_KEY, actualKeyVers);

        when(owners.size()).thenReturn(4);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckNeeded
            = ConsistencyCheckUtils.checkConflicts(oldKeys, actualKeys, cctx, null);

        assertEquals(0, recheckNeeded.size());
    }

    /**
     *
     */
    @Test
    public void testOldKeySizeLessThenOwnerAndAnyActualKeyIsGreaterThenOldMax() {
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> oldKeys = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeyVers = new HashMap<>();
        oldKeyVers.put(NODE_1, version(3));
        oldKeyVers.put(NODE_2, version(2));
        oldKeyVers.put(NODE_3, version(1));
        oldKeys.put(TEST_KEY, oldKeyVers);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys = new HashMap<>();
        Map<UUID, VersionedValue> actualKeyVers = new HashMap<>();
        actualKeyVers.put(NODE_4, versionedValue(2));
        actualKeyVers.put(NODE_1, versionedValue(3));
        actualKeyVers.put(NODE_2, versionedValue(5));
        actualKeys.put(TEST_KEY, actualKeyVers);

        when(owners.size()).thenReturn(4);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckNeeded
            = ConsistencyCheckUtils.checkConflicts(oldKeys, actualKeys, cctx, null);

        assertEquals(0, recheckNeeded.size());
    }

    /**
     *
     */
    private GridCacheVersion version(int ver) {
        return new GridCacheVersion(1, 0, ver);
    }

    /**
     *
     */
    private VersionedValue versionedValue(int ver) {
        return new VersionedValue(null, new GridCacheVersion(1, 0, ver), 1, 1);
    }
}
