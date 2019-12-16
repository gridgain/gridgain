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
import org.apache.ignite.console.AbstractSelfTest;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.session.ExpiringSession;
import org.springframework.session.FindByIndexNameSessionRepository;

import static java.util.concurrent.TimeUnit.DAYS;
import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.console.common.Utils.SPRING_SECURITY_CONTEXT;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_CREATE_BY_ADMIN;
import static org.apache.ignite.console.event.AccountEventType.ACCOUNT_DELETE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Admin service test.
 */
public class AdminServiceTest extends AbstractSelfTest {
    /** */
    private static final long MILLIS_IN_A_DAY = DAYS.toMillis(1);

    /** Activities service. */
    @Autowired
    private AdminService adminSrvc;

    /** Event publisher. */
    @MockBean
    private EventPublisher evtPublisher;

    /** Sessions. */
    @Autowired
    private FindByIndexNameSessionRepository<ExpiringSession> sesRepo;

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

        ExpiringSession ses = createSession(acc);

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

    /**
     * @param acc Account.
     */
    private ExpiringSession createSession(Account acc) {
        ExpiringSession ses = sesRepo.createSession();

        SecurityContext ctx = SecurityContextHolder.createEmptyContext();

        ses.setAttribute(SPRING_SECURITY_CONTEXT, ctx);

        Authentication auth = new UsernamePasswordAuthenticationToken(acc, acc.getPassword());

        ctx.setAuthentication(auth);

        return ses;
    }
}
