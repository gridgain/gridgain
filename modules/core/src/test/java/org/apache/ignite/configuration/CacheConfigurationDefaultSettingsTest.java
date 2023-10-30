/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JCACHE_COMPLIANCE;

/**
 * Tests value of {@link CacheConfiguration.isStatisticsEnabled}} depends on {@link IGNITE_JCACHE_COMPLIANCE}.
 */
public class CacheConfigurationDefaultSettingsTest extends GridCommonAbstractTest {
    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JCACHE_COMPLIANCE, value = "true")
    public void testIgniteJcacheComplianceSystemPropertyTrueDefaultConstructor() {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        assertFalse(cfg.isStatisticsEnabled());
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JCACHE_COMPLIANCE, value = "true")
    public void testIgniteJcacheComplianceSystemPropertyTrueConstructorWithCacheName() {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        assertFalse(cfg.isStatisticsEnabled());
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JCACHE_COMPLIANCE, value = "false")
    public void testIgniteJcacheComplianceSystemPropertyFalseDefaultConstructor() {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        assertTrue(cfg.isStatisticsEnabled());
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JCACHE_COMPLIANCE, value = "false")
    public void testIgniteJcacheComplianceSystemPropertyFalseConstructorWithCacheName() {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        assertTrue(cfg.isStatisticsEnabled());
    }

    /** */
    @Test
    public void testDefaultConstructor() {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        assertTrue(cfg.isStatisticsEnabled());
    }

    /** */
    @Test
    public void testIgniteConstructorWithCacheName() {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        assertTrue(cfg.isStatisticsEnabled());
    }
}
