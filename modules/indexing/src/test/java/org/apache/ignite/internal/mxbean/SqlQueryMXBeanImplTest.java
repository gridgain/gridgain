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

package org.apache.ignite.internal.mxbean;

import javax.management.ObjectName;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests the SqlQueryMXBeanImpl JMX properties
 */
public class SqlQueryMXBeanImplTest extends GridCommonAbstractTest {

    /** Create test and auto-start the grid */
    public SqlQueryMXBeanImplTest() {
        super(true);
    }

    /**
     * {@inheritDoc}
     *
     * set the appropriate SQL Config attrs to test for.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration().setSqlGlobalMemoryQuota("20m").setSqlQueryMemoryQuota("10m").
        setLongQueryWarningTimeout(30 * 1000L).setSqlOffloadingEnabled(true));

        return cfg;
    }


    /**
     *  tests that the JMX attributes are equal to that given in the config.
     */
    @Test
    public void testSqlQueryMXBeanImplJMXAttrs() throws Exception {
        checkBean("SQL Query", "SqlQueryMXBeanImpl", "SqlQueryMemoryQuotaBytes", 10485760L);
        checkBean("SQL Query", "SqlQueryMXBeanImpl", "SqlGlobalMemoryQuotaBytes", 20971520L);
        checkBean("SQL Query", "SqlQueryMXBeanImpl", "SqlFreeMemoryBytes", 20971520L);
        checkBean("SQL Query", "SqlQueryMXBeanImpl", "SqlQueryMemoryQuota", "10m");
        checkBean("SQL Query", "SqlQueryMXBeanImpl", "SqlGlobalMemoryQuota", "20m");

        checkBean("SQL Query", "SqlQueryMXBeanImpl", "LongQueryWarningTimeout", 30000L);
        checkBean("SQL Query", "SqlQueryMXBeanImpl", "SqlOffloadingEnabled", true);

    }

    /** Checks that a bean with the specified group and name is available and has the expected attribute */
    private void checkBean(String grp, String name, String attributeName, Object expAttributeVal) throws Exception {
        ObjectName mBeanName = IgniteUtils.makeMBeanName(grid().name(), grp, name);
        Object attributeVal = grid().configuration().getMBeanServer().getAttribute(mBeanName, attributeName);

        assertEquals(expAttributeVal, attributeVal);
    }
}
