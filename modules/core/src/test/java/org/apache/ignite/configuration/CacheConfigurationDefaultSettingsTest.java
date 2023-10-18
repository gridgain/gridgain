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

package org.apache.ignite.configuration;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests default settings of the {@link CacheConfiguration}.
 * These tests are analog of
 * {@link javax.cache.configuration.ConfigurationTest#testValidateFromBasicConfigurationRetrievedFromCache} test.
 * It was replaced because default value of {@link CacheConfiguration.isStatisticsEnabled} is true.
 */
public class CacheConfigurationDefaultSettingsTest {
    private static final String CACHE_NAME_1 = "cache_name_1";

    private static final String CACHE_NAME_2 = "cache_name_2";

    private CacheManager cacheManager;

    /**
     * Tests default settings of the {@link CacheConfiguration}.
     */
    @Test
    public void testCacheConfigurationDefaultConstructor() {
        checkCacheConfiguration(new CacheConfiguration<>());
    }

    /**
     * Tests default settings of the {@link CacheConfiguration}.
     */
    @Test
    public void testCacheConfigurationConstructorWithCacheName() {
        checkCacheConfiguration(new CacheConfiguration<>(CACHE_NAME_1));
    }

    @Before
    public void beforeTest() {
        cacheManager = Caching.getCachingProvider().getCacheManager();
    }

    @After
    public void afterTest() {
        cacheManager.destroyCache(CACHE_NAME_2);
    }

    /**
     * Ensure that a {@link MutableConfiguration} correctly uses the defaults
     * from an implementation of the base Configuration interface.
     */
    private void checkCacheConfiguration(CacheConfiguration cfg) {
        Cache<Object, Object> cache = cacheManager.createCache(CACHE_NAME_2, cfg);

        CompleteConfiguration newCfg = cache.getConfiguration(CompleteConfiguration.class);

        validateDefaults(newCfg);
    }

    /**
     * This method was copied from {@link javax.cache.configuration.ConfigurationTest} test.
     *
     * @param cfg configuration
     */
    private void validateDefaults(CompleteConfiguration<?, ?> cfg) {
        assertEquals(Object.class, cfg.getKeyType());
        assertEquals(Object.class, cfg.getValueType());
        assertFalse(cfg.isReadThrough());
        assertFalse(cfg.isWriteThrough());
        assertTrue(cfg.isStoreByValue());
        assertTrue(cfg.isStatisticsEnabled());
        assertFalse(cfg.isManagementEnabled());
        assertTrue(getConfigurationCacheEntryListenerConfigurationSize(cfg) == 0);
        assertNull(cfg.getCacheLoaderFactory());
        assertNull(cfg.getCacheWriterFactory());

        //expiry policies
        ExpiryPolicy expiryPolicy = cfg.getExpiryPolicyFactory().create();
        assertTrue(expiryPolicy instanceof EternalExpiryPolicy);
        assertThat(Duration.ETERNAL, equalTo(expiryPolicy.getExpiryForCreation()));
        assertThat(expiryPolicy.getExpiryForAccess(), is(nullValue()));
        assertThat(expiryPolicy.getExpiryForUpdate(), is(nullValue()));
    }

    private int getConfigurationCacheEntryListenerConfigurationSize(CompleteConfiguration<?, ?> cfg) {
        int i = 0;

        for (Object listenerConfig : cfg.getCacheEntryListenerConfigurations()) {
            i++;
        }

        return i;
    }
}
