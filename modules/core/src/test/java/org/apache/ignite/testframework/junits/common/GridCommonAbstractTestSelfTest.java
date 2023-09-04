package org.apache.ignite.testframework.junits.common;

import junit.framework.TestCase;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.junit.Test;

import java.util.function.Consumer;

import static org.apache.ignite.testframework.config.GridTestProperties.findTestResource;

public class GridCommonAbstractTestSelfTest extends TestCase {

    /**
     *
     */
    @Test
    public void testResourceLocation() {
        Consumer<String> check = (res) -> assertNotNull("Required resource not found: " + res, findTestResource(res));

        check.accept(GridTestProperties.TESTS_PROP_FILE);
        check.accept(GridTestProperties.DEFAULT_LOG4J_FILE);
    }
}
