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

package org.apache.ignite.internal.encryption;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 */
public class EncryptedCacheGroupCreateTest extends AbstractEncryptionTest {
    /** */
    public static final String ENCRYPTED_GROUP = "encrypted-group";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        IgniteEx igniteEx = startGrid(0);

        startGrid(1);

        igniteEx.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateEncryptedCacheGroup() throws Exception {
        KeystoreEncryptionKey key = createEncryptedCache(ENCRYPTED_CACHE, ENCRYPTED_GROUP);

        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(ENCRYPTED_CACHE + "2");

        ccfg.setEncryptionEnabled(true);
        ccfg.setGroupName(ENCRYPTED_GROUP);

        IgniteEx grid = grid(0);

        grid.createCache(ccfg);

        IgniteInternalCache<Object, Object> encrypted2 = grid.cachex(ENCRYPTED_CACHE + "2");

        GridEncryptionManager encMgr = encrypted2.context().kernalContext().encryption();

        GroupKey grpKey2 = encMgr.getActiveKey(CU.cacheGroupId(ENCRYPTED_CACHE, ENCRYPTED_GROUP));

        assertNotNull(grpKey2);

        KeystoreEncryptionKey key2 = (KeystoreEncryptionKey)grpKey2.key();

        assertNotNull(key2);
        assertNotNull(key2.key());

        assertEquals(key.key(), key2.key());
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateNotEncryptedCacheInEncryptedGroupFails() throws Exception {
        createEncryptedCache(ENCRYPTED_CACHE + "3", ENCRYPTED_GROUP + "3");

        IgniteEx grid = grid(0);

        GridTestUtils.assertThrowsWithCause(() -> {
            grid.createCache(new CacheConfiguration<>(ENCRYPTED_CACHE + "4")
                .setEncryptionEnabled(false)
                .setGroupName(ENCRYPTED_GROUP + "3"));
        }, IgniteCheckedException.class);
    }

    /** */
    private KeystoreEncryptionKey createEncryptedCache(String cacheName, String grpName) {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setEncryptionEnabled(true);
        ccfg.setGroupName(grpName);

        IgniteEx grid = grid(0);

        grid.createCache(ccfg);

        IgniteInternalCache<Object, Object> enc = grid.cachex(cacheName);

        assertNotNull(enc);

        GroupKey grpKey = grid.context().encryption().getActiveKey(CU.cacheGroupId(cacheName, grpName));

        assertNotNull(grpKey);

        KeystoreEncryptionKey key = (KeystoreEncryptionKey)grpKey.key();

        assertNotNull(key);
        assertNotNull(key.key());

        return key;
    }
}
