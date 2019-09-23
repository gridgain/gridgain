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

package org.gridgain;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.gmc.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.gridgain.config.TestChannelInterceptor;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Abstract grid with agent test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public abstract class AbstractGridWithAgentTest extends GridCommonAbstractTest {
    /** Template. */
    @Autowired
    protected SimpMessagingTemplate template;

    /** Interceptor. */
    @Autowired
    protected TestChannelInterceptor interceptor;

    /** Port. */
    @LocalServerPort
    protected int port;

    /** Cluster. */
    protected IgniteCluster cluster;

    /**
     * Stop all grids and clear persistence dir.
     */
    @After
    public void stopAndClear() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * @param ignite Ignite.
     */
    protected void changeGmcUri(IgniteEx ignite) {
        ManagementConfiguration cfg = ignite.context().gmc().configuration();
        cfg.setServerUris(F.asList("http://localhost:" + port));

        ignite.context().gmc().configuration(cfg);
    }
}
