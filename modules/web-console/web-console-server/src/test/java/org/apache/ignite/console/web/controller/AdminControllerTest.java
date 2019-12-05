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

import java.util.Collection;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.AdminService;
import org.apache.ignite.console.web.model.SignInRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.model.UserResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.session.ExpiringSession;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;
import static org.apache.ignite.console.utils.Utils.toJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

/**
 * Tests for admin controller.
 */
@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@SpringBootTest(classes = {TestGridConfiguration.class}, webEnvironment = RANDOM_PORT)
public class AdminControllerTest {
    /** Administration email. */
    private static final String ADMIN_EMAIL = "admin@test.com";

    /** User email. */
    private static final String USER_EMAIL = "user@test.com";

    /** Rest template. */
    @Autowired
    private TestRestTemplate restTemplate;

    /** Account controller. */
    @Autowired
    private AccountsService accountSrvc;

    /** Admin service. */
    @Autowired
    private AdminService adminSrvc;

    /** Session repository. */
    @Autowired
    private FindByIndexNameSessionRepository<ExpiringSession> sesRepo;
    
    /** Latest cookies. */
    private String latestCookies;

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
        cleanup();
    }

    /** */
    @After
    public void cleanup() {
        latestCookies = null;
        
        deleteUser(ADMIN_EMAIL);
        deleteUser(USER_EMAIL);
    }

    /** */
    @Test
    public void shouldNotUpdateLastFieldsOnBecameThisUser() {
        registerUser(ADMIN_EMAIL, true);
        registerUser(USER_EMAIL, false);

        assertTrue(sessions(ADMIN_EMAIL).isEmpty());
        assertTrue(sessions(USER_EMAIL).isEmpty());

        SignInRequest req = new SignInRequest();

        req.setEmail(ADMIN_EMAIL);
        req.setPassword("password");

        request("/api/v1/signin", HttpMethod.POST, toJson(req), Void.class);

        UserResponse user = request("/api/v1/user", HttpMethod.GET, null, UserResponse.class);

        assertEquals(ADMIN_EMAIL, user.getEmail());
        assertTrue(user.isAdmin());
        assertFalse(user.isBecomeUsed());

        assertEquals(1, sessions(ADMIN_EMAIL).size());
        assertTrue(sessions(USER_EMAIL).isEmpty());

        request("/api/v1/admin/login/impersonate?email=" + USER_EMAIL , HttpMethod.GET, null, Void.class);

        user = request("/api/v1/user", HttpMethod.GET, null, UserResponse.class);

        assertEquals(USER_EMAIL, user.getEmail());
        assertFalse(user.isAdmin());
        assertTrue(user.isBecomeUsed());

        assertEquals(1, sessions(ADMIN_EMAIL).size());
        assertEquals(ADMIN_EMAIL, F.first(sessions(ADMIN_EMAIL)).getAttribute(PRINCIPAL_NAME_INDEX_NAME));
        assertTrue(sessions(USER_EMAIL).isEmpty());
    }

    /**
     * @param url the URL
     * @param mtd the HTTP method (GET, POST, etc)
     * @param body the entity body
     * @param resType the type of the return value
     */
    private <T> T request(String url, HttpMethod mtd, String body, Class<T> resType) {
        HttpHeaders headers = new HttpHeaders();
        
        headers.set("Cookie", latestCookies);
        
        HttpEntity<String> reqEntity = new HttpEntity<>(body, headers);

        ResponseEntity<T> res = restTemplate.exchange(url, mtd, reqEntity, resType);

        if (res.getHeaders().get("Set-Cookie") != null)
            latestCookies = F.first(res.getHeaders().get("Set-Cookie"));

        return res.getBody();
    }

    /**
     * @param email Email.
     */
    private Collection<ExpiringSession> sessions(String email) {
        return sesRepo.findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME, email).values();
    }

    /**
     * @param email Email.
     * @param adminFlag Administration flag.
     */
    private void registerUser(String email, boolean adminFlag) {
        SignUpRequest req = new SignUpRequest();

        req.setEmail(email);
        req.setPassword("password");

        Account acc = adminSrvc.registerUser(req);

        accountSrvc.toggle(acc.getId(), adminFlag);
    }

    /**
     * @param email Email.
     */
    private void deleteUser(String email) {
        try {
            Account acc = accountSrvc.loadUserByUsername(email);

            adminSrvc.delete(acc.getId());
        }
        catch (UsernameNotFoundException ignored) {
            // No-op.
        }
    }
}
