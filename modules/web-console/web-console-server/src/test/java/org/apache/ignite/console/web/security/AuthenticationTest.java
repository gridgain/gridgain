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

package org.apache.ignite.console.web.security;

import java.util.UUID;
import org.apache.ignite.console.AbstractSelfTest;
import org.apache.ignite.console.config.AccountAuthenticationConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;

import static org.apache.ignite.console.messages.WebConsoleMessageSource.message;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for authentication.
 */
@SpringBootTest(properties = {"account.authentication.maxAttempts=20"})
public class AuthenticationTest extends AbstractSelfTest {
    /** User email. */
    private static final String USER_EMAIL = "user@test.com";

    /** Authentication manager. */
    @Autowired
    private AuthenticationManager authMgr;

    /** Password encoder. */
    @Autowired
    private PasswordEncoder encoder;

    /** Accounts repository. */
    @Autowired
    private AccountsRepository accountsRepo;

    /** Account authentication configuration. */
    @Autowired
    private AccountAuthenticationConfiguration cfg;

    /** Should supply message when limiting attempts and authenticating too soon. */
    @Test
    public void shouldLockAuthenticateWithTooManyAttempts() {
        createUser(USER_EMAIL, 20, U.currentTimeMillis());

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "password"
            ));

            return null;
        }, LockedException.class, message("err.account-too-many-attempts"));
    }

    /** Should lock authenticate after too many login attempts. */
    @Test
    public void shouldLockAuthenticateWithAttemptTooSoon() throws InterruptedException {
        createUser(USER_EMAIL, 2, U.currentTimeMillis());

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "password"
            ));

            return null;
        }, LockedException.class, message("err.account-attempt-too-soon"));

        Thread.sleep((long)Math.pow(cfg.getInterval(), Math.log(4)));

        assertNotNull(authMgr.authenticate(new UsernamePasswordAuthenticationToken(
            USER_EMAIL,
            "password"
        )));
    }

    /**
     * @param email Email.
     * @param attemptsCnt Failed attempt count.
     * @param lastFailedLogin Last failed attempt.
     */
    private void createUser(String email, int attemptsCnt, long lastFailedLogin) {
        Account acc = new Account();

        acc.setId(UUID.randomUUID());
        acc.setToken(UUID.randomUUID().toString());
        acc.setEmail(email);
        acc.setPassword(encoder.encode("password"));
        acc.setAttemptsCount(attemptsCnt);
        acc.setLastFailedLogin(lastFailedLogin);

        accountsRepo.save(acc);
    }
}
