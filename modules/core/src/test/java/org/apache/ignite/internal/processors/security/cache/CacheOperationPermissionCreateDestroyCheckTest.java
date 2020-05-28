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

package org.apache.ignite.internal.processors.security.cache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test create and destroy cache permissions.
 */
@RunWith(Parameterized.class)
public class CacheOperationPermissionCreateDestroyCheckTest extends AbstractSecurityTest {
    /** */
    @Parameterized.Parameters(name = "clientMode={0}")
    public static Iterable<Boolean[]> data() {
        return Arrays.asList(new Boolean[] {true}, new Boolean[] {false});
    }

    /** */
    private static final String SRV = "srv";

    /** */
    private static final String SRV_WITHOUT_PERMS = "srv_without_perms";

    /** */
    private static final String CLNT_WITHOUT_PERMS = "clnt_without_perms";

    /** */
    private static final String TEST_NODE = "test_node";

    /** */
    private static final String CACHE_NAME = "CACHE_NAME";

    /** */
    private static final String UNMANAGED_CACHE = "UNMANAGED_CACHE";

    /** */
    @Parameterized.Parameter()
    public boolean clientMode;

    /**
     * Checks that cache create is authorized if CACHE_CREATE is set as cache-level permission.
     * Steps:
     * 1. Configure a user with permission to create a specific cache.
     * 2. User joins as a cluster node.
     * 3. Attempt to create cache with name from #1 is successful.
     */
    @Test
    public void testCreateCacheWithCachePermissions() throws Exception {
        SecurityPermissionSet secPermSet = builder()
            .appendCachePermissions(CACHE_NAME, CACHE_CREATE)
            .build();

        try (Ignite node = startGrid(TEST_NODE, secPermSet, clientMode)) {
            assertThrowsWithCause(() -> node.createCache(UNMANAGED_CACHE), SecurityException.class);

            assertNull(grid(SRV).cache(UNMANAGED_CACHE));

            assertNotNull(node.createCache(CACHE_NAME));
        }
    }

    /**
     * Checks that cache destroy is authorized if CACHE_DESTROY is set as cache-level permission.
     * Steps:
     * 1. Configure a user with permission to destroy a specific cache.
     * 2. Existing server creates two caches: one with name from #1 and another with different name.
     * 3. User joins as a cluster node.
     * 4. User attempt to destroy cache with different name fails.
     * 5. User attempt to destroy cache with name from #1 is successful.
     */
    @Test
    public void testDestroyCacheWithCachePermissions() throws Exception {
        SecurityPermissionSet secPermSet = builder()
            .appendCachePermissions(CACHE_NAME, CACHE_DESTROY)
            .build();

        grid(SRV).createCache(CACHE_NAME);
        grid(SRV).createCache(UNMANAGED_CACHE);

        try (Ignite node = startGrid(TEST_NODE, secPermSet, clientMode)) {
            node.destroyCache(CACHE_NAME);

            assertThrowsWithCause(() -> node.destroyCache(UNMANAGED_CACHE), SecurityException.class);

            assertNull(grid(SRV).cache(CACHE_NAME));

            assertNotNull(grid(SRV).cache(UNMANAGED_CACHE));
        }
    }

    /**
     * Checks that cache create is authorized if CACHE_CREATE is set as system permission.
     * Steps:
     * 1. Configure a user with system permission to create caches.
     * 2. Configure another user without such permission.
     * 3. Node with #2 permissions fails to create a cache.
     * 4. Node with #1 permissions successfully creates a cache.
     */
    @Test
    public void testCreateCacheWithSystemPermissions() throws Exception {
        SecurityPermissionSet secPermSet = builder()
            .appendSystemPermissions(CACHE_CREATE)
            .build();

        try (Ignite node = startGrid(TEST_NODE, secPermSet, clientMode)) {
            assertThrowsWithCause(() -> forbidden(clientMode).createCache(CACHE_NAME), SecurityException.class);

            assertNotNull(node.createCache(CACHE_NAME));
        }
    }

    /**
     * Checks that cache destroy is authorized if CACHE_DESTROY is set as system permission.
     * Steps:
     * 1. Configure a user with system permission to destroy caches.
     * 2. Configure another user without such permission.
     * 3. Existing server creates two caches: one with name from #1 and another with different name.
     * 4. Node with #2 permissions fails to destroy a cache.
     * 5. Node with #1 permissions successfully destroys a cache.
     */
    @Test
    public void testDestroyCacheWithSystemPermissions() throws Exception {
        SecurityPermissionSet secPermSet = builder()
            .appendSystemPermissions(CACHE_DESTROY)
            .build();

        grid(SRV).createCache(CACHE_NAME);

        try (Ignite node = startGrid(TEST_NODE, secPermSet, clientMode)) {
            assertThrowsWithCause(() -> forbidden(clientMode).destroyCache(CACHE_NAME), SecurityException.class);

            node.destroyCache(CACHE_NAME);

            assertNull(grid(SRV).cache(CACHE_NAME));
        }
    }

    /** */
    private SecurityPermissionSetBuilder builder() {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(JOIN_AS_SERVER);
    }

    /**
     * @param isClnt Is client.
     */
    private Ignite forbidden(boolean isClnt) {
        return isClnt ? grid(CLNT_WITHOUT_PERMS) : grid(SRV_WITHOUT_PERMS);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV);

        startGrid(CLNT_WITHOUT_PERMS, builder().build(), true);

        startGrid(SRV_WITHOUT_PERMS, builder().build(), false);

        grid(SRV).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Ignite server = grid(SRV);

        server.cacheNames().forEach(server::destroyCache);

        if (!clientMode)
            cleanPersistenceDir(TEST_NODE);
    }
}
