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

package org.apache.ignite.console.services;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.security.IgniteSessionRepository;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.session.ExpiringSession;
import org.springframework.test.context.junit4.SpringRunner;

import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.console.common.Utils.SPRING_SECURITY_CONTEXT;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE_BY_ADMIN;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_DELETE;
import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Admin service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestGridConfiguration.class})
public class AdminServiceTest {
    /** */
    private static final long MILLIS_IN_A_DAY = 86400000;

    /** Activities service. */
    @Autowired
    private AdminService adminSrvc;

    /** Event publisher. */
    @MockBean
    private EventPublisher evtPublisher;

    /** Sessions. */
    @Autowired
    private IgniteSessionRepository sesRepo;

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

    /**
     * Should publish event with ACCOUNT_DELETE type.
     */
    @Test
    public void shouldPublishUserCreareAndDeleteEvent() {
        Account acc = adminSrvc.registerUser(signUpRequest("delete@delete.com"));

        adminSrvc.delete(acc.getId());

        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        verify(evtPublisher, times(2)).publish(captor.capture());

        assertEquals(ACCOUNT_DELETE, captor.getValue().getType());
    }

    /**
     * @return Sign up request.
     */
    private SignUpRequest signUpRequest(String email) {
        SignUpRequest req = new SignUpRequest();
        req.setEmail(email);
        req.setPassword("1");

        return req;
    }

    /**
     * Should publish event with ACCOUNT_CREATE_BY_ADMIN type.
     */
    @Test
    public void shouldPublishUserCreateByAdminEvent() {
        adminSrvc.registerUser(signUpRequest("create@create.com"));

        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        assertEquals(ACCOUNT_CREATE_BY_ADMIN, captor.getValue().getType());
    }

    /**
     * Should list users.
     */
    @Test
    public void shouldListUsers() {
        Account acc = adminSrvc.registerUser(signUpRequest("test@test.com"));

        ExpiringSession ses = sesRepo.createSession();

        Authentication auth = new UsernamePasswordAuthenticationToken(acc, acc.getPassword());

        SecurityContext ctx = SecurityContextHolder.createEmptyContext();

        ctx.setAuthentication(auth);

        ses.setAttribute(SPRING_SECURITY_CONTEXT, ctx);

        sesRepo.save(ses);

        long now = System.currentTimeMillis();

        JsonArray list = adminSrvc.list(now - MILLIS_IN_A_DAY, now + MILLIS_IN_A_DAY);

        AtomicInteger cnt = new AtomicInteger(0);

        list.forEach(item -> {
            JsonObject json = (JsonObject)item;

            if ("test@test.com".equals(json.getString("email"))) {
                cnt.incrementAndGet();

                assertTrue(json.getLong("lastLogin", -1L) > 0);
                assertTrue(json.getLong("lastActivity", -1L) > 0);
            }
        });

        assertEquals(1, cnt.get());
    }
}
