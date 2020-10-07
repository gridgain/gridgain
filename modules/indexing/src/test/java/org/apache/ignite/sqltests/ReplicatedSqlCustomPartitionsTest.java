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

package org.apache.ignite.sqltests;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Includes all base sql test plus tests that make sense in replicated mode.
 */
public class ReplicatedSqlCustomPartitionsTest extends ReplicatedSqlTest {
    /** Name of the department table created in partitioned mode. */
    private String DEP_PART_TAB = "DepartmentPart";

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration("partitioned11*")
                    .setAffinity(new RendezvousAffinityFunction(false, 11)),
                new CacheConfiguration("replicated11*")
                    .setCacheMode(CacheMode.REPLICATED)
                    .setAffinity(new RendezvousAffinityFunction(false, 11))
            );
    }

    /**
     * Create and fill common tables in replicated mode.
     * Also create additional department table in partitioned mode to
     * test mixed partitioned/replicated scenarios.
     */
    @Override protected void setupData() {
        createTables("template=replicated11");

        fillCommonData();

        createDepartmentTable("DepartmentPart", "template=partitioned11");

        fillDepartmentTable("DepartmentPart");
    }
}
