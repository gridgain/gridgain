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

package org.apache.ignite.testframework.junits.common;

import junit.framework.TestCase;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.junit.Test;

import java.util.function.Consumer;

import static org.apache.ignite.testframework.config.GridTestProperties.findTestResource;

/**
 * Tests for {@link GridCommonAbstractTest}.
 */
public class GridCommonAbstractTestSelfTest extends TestCase {
    /**
     * Tests that all required resources are available from classpath.
     */
    @Test
    public void testResourceLocation() {
        Consumer<String> check = (res) -> assertNotNull("Required resource not found: " + res, findTestResource(res));

        check.accept(GridTestProperties.TESTS_PROP_FILE);
        check.accept(GridTestProperties.DEFAULT_LOG4J_FILE);
    }
}
