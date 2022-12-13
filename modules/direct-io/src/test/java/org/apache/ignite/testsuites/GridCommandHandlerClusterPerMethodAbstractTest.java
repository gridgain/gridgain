package org.apache.ignite.testsuites;

public abstract class GridCommandHandlerClusterPerMethodAbstractTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }
}
