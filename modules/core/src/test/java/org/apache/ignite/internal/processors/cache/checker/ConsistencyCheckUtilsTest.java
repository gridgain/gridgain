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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ConsistencyCheckUtilsTest {
    /**
     *
     */
    @Test
    public void testCheckConsistency() {
        UUID node1 = UUID.randomUUID();
        UUID node2 = UUID.randomUUID();
        UUID node3 = UUID.randomUUID();
        UUID node4 = UUID.randomUUID();

        Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
        oldKey.put(node1, version(1));
        oldKey.put(node2, version(3));
        oldKey.put(node3, version(3));
        oldKey.put(node4, version(2));

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>(); // All keys was removed

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node1, versionedValue(1));
            actualKey.put(node2, versionedValue(3));
            actualKey.put(node3, versionedValue(4)); // Max version increase
            actualKey.put(node4, versionedValue(2));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node1, versionedValue(1));
            actualKey.put(node2, versionedValue(3)); // Max of node 3 was removed
            actualKey.put(node4, versionedValue(2));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node1, versionedValue(3)); // Min value like max
            actualKey.put(node2, versionedValue(3));
            actualKey.put(node3, versionedValue(3));
            actualKey.put(node4, versionedValue(3));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node1, versionedValue(4)); // Min value greater then max
            actualKey.put(node2, versionedValue(3));
            actualKey.put(node3, versionedValue(3));
            actualKey.put(node4, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node1, versionedValue(1)); // Nothing changed.
            actualKey.put(node2, versionedValue(3));
            actualKey.put(node3, versionedValue(3));
            actualKey.put(node4, versionedValue(2));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node1, versionedValue(2)); // Not all min values were incremented.
            actualKey.put(node2, versionedValue(3));
            actualKey.put(node3, versionedValue(3));
            actualKey.put(node4, versionedValue(3));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node2, versionedValue(3)); // Remove of one value is not enough
            actualKey.put(node3, versionedValue(3));
            actualKey.put(node4, versionedValue(3));

            assertFalse(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }
    }

    /**
     *
     */
    @Test
    public void testCheckConsistencyMaxGroup() {
        UUID node1 = UUID.randomUUID();
        UUID node2 = UUID.randomUUID();
        UUID node3 = UUID.randomUUID();

        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(node1, version(1));
            oldKey.put(node2, version(1));

            {
                Map<UUID, VersionedValue> actualKey = new HashMap<>();
                actualKey.put(node1, versionedValue(1));

                assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
            }

            {
                Map<UUID, VersionedValue> actualKey = new HashMap<>();
                actualKey.put(node2, versionedValue(1));

                assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
            }
        }

        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(node1, version(1));
            oldKey.put(node2, version(2));
            oldKey.put(node3, version(3));

            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node3, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }

        {
            Map<UUID, GridCacheVersion> oldKey = new HashMap<>();
            oldKey.put(node1, version(3));
            oldKey.put(node2, version(2));
            oldKey.put(node3, version(1));

            Map<UUID, VersionedValue> actualKey = new HashMap<>();
            actualKey.put(node1, versionedValue(4));

            assertTrue(ConsistencyCheckUtils.checkConsistency(oldKey, actualKey));
        }
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
