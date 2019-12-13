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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.console.TestGridConfiguration;
import org.apache.ignite.console.common.SessionAttribute;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.web.security.IgniteSessionRepository;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.session.ExpiringSession;
import org.springframework.session.SessionRepository;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.ignite.console.common.Utils.SPRING_SECURITY_CONTEXT;
import static org.apache.ignite.console.utils.TestUtils.cleanPersistenceDir;
import static org.apache.ignite.console.utils.TestUtils.stopAllGrids;
import static org.junit.Assert.assertTrue;

/**
 * Session service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestGridConfiguration.class})
public class SessionsServiceTest {
    /** */
    @Autowired
    private SessionsService sesSrvc;

    /** */
    @Autowired
    private SessionRepository<ExpiringSession> sesRepo;

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
     * Should change session attribute.
     */
    @Test
    public void interactionWithAttribute() {
        String testAttrName = "test_attr";

        ExpiringSession ses = sesRepo.createSession();

        Assert.assertNull(ses.getAttribute(testAttrName));

        sesRepo.save(ses);

        SessionAttribute attr = new SessionAttribute(ses.getId(), testAttrName);

        Assert.assertNull(sesSrvc.get(attr));

        sesSrvc.update(attr, "val");

        Assert.assertEquals("val", sesSrvc.get(attr));

        sesSrvc.update(attr, "val1");

        Assert.assertEquals("val1", sesSrvc.get(attr));

        sesSrvc.remove(attr);

        Assert.assertNull(sesSrvc.get(attr));
    }

    /**
     * Should check locks order in {@link IgniteSessionRepository#save}, {@link SessionsService#update}.
     */
    @Test
    public void checkPossibleRace() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);

        ExpiringSession ses = createSession(new Account("test@test.com", null, null, null, null, null, null));

        pool.submit(() -> {
            for (int i = 0; i < 10; i++)
                sesRepo.save(ses);
        });

        pool.submit(() -> {
            SessionAttribute attr = new SessionAttribute(ses.getId(), "testAttribute");

            for (int i = 0; i < 10; i++)
                sesSrvc.update(attr, "test" + i);
        });

        pool.shutdown();

        assertTrue(pool.awaitTermination(1L, TimeUnit.SECONDS));
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
