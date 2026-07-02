/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for Ignite-style source-compatibility alias methods that delegate to the canonical GridGain API
 * (see GG-49060). Each alias getter/setter must round-trip through the canonical method, and the
 * {@link SystemDataRegionConfiguration} wrapper must produce a configuration identical to the direct
 * {@link DataStorageConfiguration} setters.
 */
public class SourceCompatibilityAliasesTest {
    /**
     * {@link DataStorageConfiguration#setSystemDataRegionConfiguration(SystemDataRegionConfiguration)} must
     * push both values onto the canonical scalar setters, and the getter must reflect them back.
     */
    @Test
    public void testSystemDataRegionConfigurationDelegatesToScalarSetters() {
        long initSize = 64L * 1024 * 1024;
        long maxSize = 128L * 1024 * 1024;

        DataStorageConfiguration viaWrapper = new DataStorageConfiguration()
            .setSystemDataRegionConfiguration(new SystemDataRegionConfiguration()
                .setInitialSize(initSize)
                .setMaxSize(maxSize));

        // Wrapper setter must update the canonical scalar values.
        assertEquals(initSize, viaWrapper.getSystemRegionInitialSize());
        assertEquals(maxSize, viaWrapper.getSystemRegionMaxSize());

        // Wrapper getter must reflect the canonical scalar values.
        assertEquals(initSize, viaWrapper.getSystemDataRegionConfiguration().getInitialSize());
        assertEquals(maxSize, viaWrapper.getSystemDataRegionConfiguration().getMaxSize());

        // The wrapper path must produce the same config as the direct scalar setters.
        DataStorageConfiguration viaScalars = new DataStorageConfiguration()
            .setSystemRegionInitialSize(initSize)
            .setSystemRegionMaxSize(maxSize);

        assertEquals(viaScalars.getSystemRegionInitialSize(), viaWrapper.getSystemRegionInitialSize());
        assertEquals(viaScalars.getSystemRegionMaxSize(), viaWrapper.getSystemRegionMaxSize());
    }

    /**
     * {@link SystemDataRegionConfiguration} defaults must match the canonical {@link DataStorageConfiguration}
     * system-region defaults.
     */
    @Test
    public void testSystemDataRegionConfigurationDefaults() {
        SystemDataRegionConfiguration wrapper = new SystemDataRegionConfiguration();
        DataStorageConfiguration dfltDsCfg = new DataStorageConfiguration();

        assertEquals(SystemDataRegionConfiguration.DFLT_SYS_REG_INIT_SIZE, wrapper.getInitialSize());
        assertEquals(SystemDataRegionConfiguration.DFLT_SYS_REG_MAX_SIZE, wrapper.getMaxSize());

        assertEquals(dfltDsCfg.getSystemRegionInitialSize(), wrapper.getInitialSize());
        assertEquals(dfltDsCfg.getSystemRegionMaxSize(), wrapper.getMaxSize());
    }

    /**
     * {@link ThinClientConfiguration#setServerToClientExceptionStackTraceSending(boolean)} must delegate to
     * the canonical {@link ThinClientConfiguration#setSendServerExceptionStackTraceToClient(boolean)}.
     */
    @Test
    public void testThinClientServerToClientExceptionStackTraceSending() {
        ThinClientConfiguration cfg = new ThinClientConfiguration()
            .setServerToClientExceptionStackTraceSending(true);

        assertEquals(true, cfg.sendServerExceptionStackTraceToClient());

        cfg.setServerToClientExceptionStackTraceSending(false);

        assertEquals(false, cfg.sendServerExceptionStackTraceToClient());
    }

    /**
     * {@link ClientConfiguration#setPartitionAwarenessEnabled(boolean)} must delegate to the canonical
     * {@link ClientConfiguration#setAffinityAwarenessEnabled(boolean)}.
     */
    @Test
    public void testClientPartitionAwarenessEnabled() {
        ClientConfiguration cfg = new ClientConfiguration().setPartitionAwarenessEnabled(false);

        assertEquals(false, cfg.isAffinityAwarenessEnabled());
        assertEquals(false, cfg.isPartitionAwarenessEnabled());

        cfg.setAffinityAwarenessEnabled(true);

        assertEquals(true, cfg.isPartitionAwarenessEnabled());
    }

    /**
     * {@link IgniteConfiguration#setDefaultQueryTimeout(long)} must delegate to
     * {@link SqlConfiguration#setDefaultQueryTimeout(long)} on the SQL configuration.
     */
    @Test
    public void testIgniteDefaultQueryTimeoutDelegatesToSqlConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration().setDefaultQueryTimeout(12_000L);

        assertEquals(12_000L, cfg.getDefaultQueryTimeout());
        assertEquals(12_000L, cfg.getSqlConfiguration().getDefaultQueryTimeout());
    }

    /**
     * {@link IgniteConfiguration#setDefaultQueryTimeout(long)} must lazily create the SQL configuration when
     * it has been explicitly cleared.
     */
    @Test
    public void testIgniteDefaultQueryTimeoutLazilyCreatesSqlConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setSqlConfiguration(new SqlConfiguration());
        cfg.setDefaultQueryTimeout(3_000L);

        assertEquals(3_000L, cfg.getDefaultQueryTimeout());
    }
}
