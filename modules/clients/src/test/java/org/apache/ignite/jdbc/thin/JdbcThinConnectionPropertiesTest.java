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

package org.apache.ignite.jdbc.thin;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import org.junit.Test;

/**
 * {@link ConnectionPropertiesImpl} unit tests.
 */
public class JdbcThinConnectionPropertiesTest extends JdbcThinAbstractSelfTest {
    /**
     * Test check the {@link ConnectionPropertiesImpl#getDriverPropertyInfo()} return properties with prefix {@link
     * ConnectionPropertiesImpl#PROP_PREFIX}
     */
    @Test
    public void testNamePrefixDriverPropertyInfo() {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();
        DriverPropertyInfo[] propsInfo = connProps.getDriverPropertyInfo();

        String propName = propsInfo[0].name;

        assertTrue(propName.startsWith(ConnectionPropertiesImpl.PROP_PREFIX));
    }

    /**
     * Test check the {@link ConnectionPropertiesImpl#init(String, Properties)} requires property names
     * with prefix {@link ConnectionPropertiesImpl#PROP_PREFIX}
     */
    @Test
    public void testPrefixedPropertiesApplicable() throws SQLException {
        final String TEST_PROP_NAME = ConnectionPropertiesImpl.PROP_PREFIX + "enforceJoinOrder";
        final Properties props = new Properties();

        props.setProperty(TEST_PROP_NAME, Boolean.TRUE.toString());
        ConnectionPropertiesImpl connPropsTrue = new ConnectionPropertiesImpl();
        connPropsTrue.init("", props);

        props.setProperty(TEST_PROP_NAME, Boolean.FALSE.toString());
        ConnectionPropertiesImpl connPropsFalse = new ConnectionPropertiesImpl();
        connPropsFalse.init("", props);

        assertTrue(connPropsTrue.isEnforceJoinOrder());
        assertFalse(connPropsFalse.isEnforceJoinOrder());
    }
}
