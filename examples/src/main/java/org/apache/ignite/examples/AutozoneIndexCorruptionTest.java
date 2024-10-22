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

package org.apache.ignite.examples;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Starts up an empty node with example compute configuration.
 */
public class AutozoneIndexCorruptionTest {

    private static final int REGION_SIZE = 64 * 1024 * 1024;

    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws IgniteException {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setMetricsEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                /*.setMaxSize(REGION_SIZE)*/)
            //.setWalPath()
        );

        cfg.setWorkDirectory("C:\\work\\bugs\\21028\\corrupted-index-bkup\\corrupted-index-bkup\\test_work_dir");

        //Ignition.start("examples/config/persistentstore/example-persistent-store.xml");
        Ignition.start(cfg);
    }
}
