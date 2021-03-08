package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Test views with persistence.
 */
public class StatisticsViewsPersistenceTest extends StatisticsViewsTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName);
    }
}
