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

package org.apache.ignite.console.web.controller;

import org.apache.ignite.console.MockUserDetailsServiceConfiguration;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.WithMockTestUser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.apache.ignite.console.MockUserDetailsServiceConfiguration.TEST_EMAIL;
import static org.apache.ignite.console.messages.WebConsoleMessageSource.message;
import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;
import static org.hamcrest.CoreMatchers.containsString;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests for agent download controller.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {MockUserDetailsServiceConfiguration.class, TestGridConfiguration.class},
    properties = {"agent.file.regexp=empty.zip"}
)
public class AgentDownloadControllerTest {
    /** Agent download controller. */
    @Autowired
    private AgentDownloadController agentDownloadController;

    /** Configuration for a web application.ø */
    @Autowired
    private WebApplicationContext ctx;

    /** Endpoint for server-side Spring MVC test support */
    private MockMvc mvc;

    /**
     * @throws Exception If failed.
     */
    @BeforeClass
    public static void setup() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @AfterClass
    public static void tearDown() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    @Before
    public void init() {
        this.mvc = MockMvcBuilders.webAppContextSetup(ctx).build();

        cleanup();
    }

    /** */
    @After
    public void cleanup() {
        ReflectionTestUtils.setField(agentDownloadController, "agentFolderName", "agent_dists");
    }

    /** */
    @Test
    @WithMockTestUser
    public void shouldReturnNotFoundMessage() throws Exception {
        this.mvc.perform(get("/api/v1/downloads/agent"))
            .andExpect(status().is5xxServerError())
            .andExpect(content().string(containsString(message("err.agent-dist-not-found"))));
    }

    /** */
    @Test
    @WithMockTestUser
    public void shouldReturnNotFoundInWorkDir() throws Exception {
        ReflectionTestUtils.setField(agentDownloadController, "agentFolderName", "src/test/resources");

        this.mvc.perform(get("/api/v1/downloads/agent").with(user(TEST_EMAIL)))
            .andExpect(status().is5xxServerError())
            .andExpect(content().string(containsString(message("err.agent-dist-not-found"))));
    }

    /** */
    @Test
    @WithMockTestUser
    public void shouldReturnArchive() throws Exception {
        ReflectionTestUtils.setField(agentDownloadController, "agentFolderName", "modules/web-console/web-console-server/src/test/resources");

        this.mvc.perform(get("/api/v1/downloads/agent").with(user(TEST_EMAIL)))
            .andExpect(status().isOk())
            .andExpect(content().contentType("application/zip"));
    }
}
