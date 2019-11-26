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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.console.MockUserDetailsServiceConfiguration;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.WithMockTestUser;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests for account controller.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {MockUserDetailsServiceConfiguration.class, TestGridConfiguration.class}
)
public class AccountControllerSelfTest {
    /** */
    private ObjectMapper mapper = new ObjectMapper();

    /** */
    @Autowired
    private AccountController accCtrl;

    /** */
    @Autowired
    private AccountsRepository accRepo;

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
        mvc = MockMvcBuilders.webAppContextSetup(ctx).build();
    }

    /***/
    private SignUpRequest signUpRequest(String email, String firstName, String lastName, String pwd) {
        SignUpRequest req = new SignUpRequest();

        if (!F.isEmpty(email))
            req.setEmail(email);

        if (!F.isEmpty(firstName))
            req.setFirstName(firstName);

        if (!F.isEmpty(lastName))
            req.setLastName(lastName);

        if (!F.isEmpty(pwd))
            req.setPassword(pwd);

        return req;
    }

    private RequestBuilder postRequest(String url, Object req) throws JsonProcessingException {
        return post(url)
            .contentType(APPLICATION_JSON)
            .content(mapper.writeValueAsString(req));
    }

    /** GG-25864: Testcase 19. */
    @Test
    @WithMockTestUser
    public void shouldRegistedAccount() throws Exception {
        SignUpRequest req = signUpRequest("good@test.com", "goodFirstName", "goodLastName", "good");

        mvc.perform(postRequest("/api/v1/signup", req))
            .andExpect(status().isOk());

        Account acc = accRepo.getByEmail("good@test.com");

        assertNotNull(acc);
        assertEquals("good@test.com", acc.getEmail());
        assertEquals("goodFirstName", acc.getFirstName());
        assertEquals("goodLastName", acc.getLastName());
        assertEquals("-", acc.getCompany());
        assertEquals("Rest of the World", acc.getCountry());
    }

    /** GG-25864: Testcase 20. */
    @Test
    @WithMockTestUser
    public void shouldNotRegisterAccount() throws Exception {
        SignUpRequest req = signUpRequest(null, "goodFirstName", "goodLastName", "good");

        mvc.perform(postRequest("/api/v1/signup", req))
            .andExpect(status().isBadRequest());

//        Account acc = accRepo.getByEmail(null);
//
//        assertNull(acc);

    }
}
