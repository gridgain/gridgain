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

import static org.apache.ignite.IgniteSystemProperties.getInteger;

/** */
public class TestDataStorageConfiguration extends DataStorageConfiguration {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Whether {@link TestDataStorageConfiguration} should be used in node configuration if
     * {@link DataStorageConfiguration} config is absent. {@code true} by default.
     */
    public static final String INIT_TEST_DS_CFG_PROPERTY = "init.test.datastorage.configuration";

    /** {@code true}, default value for {@link #INIT_TEST_DS_CFG_PROPERTY}.*/
    public static final boolean DFLT_INIT_TEST_DS_CFG = true;

    /** Default wal segments count for {@link TestDataStorageConfiguration}. */
    public static final String TEST_WAL_SEGMENTS_PROPERTY = "test.wal.segments";

    /** Default wal segment size for {@link TestDataStorageConfiguration}. */
    public static final String TEST_WAL_SEGMENT_SIZE_PROPERTY = "test.wal.segment.size";

    /** Default test wal segments count for {@link TestDataStorageConfiguration}. */
    public static final int DFLT_TEST_WAL_SEGMENTS = 5;

    /** Default test wal segment size for {@link TestDataStorageConfiguration}. */
    public static final int DFLT_TEST_WAL_SEGMENT_SIZE = 4 * 1024 * 1024;

    /**
     * Default constructor.
     */
    public TestDataStorageConfiguration() {
        setWalSegments(getInteger(TEST_WAL_SEGMENTS_PROPERTY, DFLT_TEST_WAL_SEGMENTS));
        setWalSegmentSize(getInteger(TEST_WAL_SEGMENT_SIZE_PROPERTY, DFLT_TEST_WAL_SEGMENT_SIZE));
    }
}
