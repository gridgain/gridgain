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
import org.apache.ignite.console.config.AccountConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.SpringSecurityMessageSource;
import org.springframework.security.crypto.password.PasswordEncoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for authentication.
 */
@SpringBootTest(properties = {"account.authentication.maxAttempts=3"})
public class AuthenticationTest extends AbstractSelfTest {
    /** Messages accessor. */
    private static final MessageSourceAccessor messages = SpringSecurityMessageSource.getAccessor();

    /** User email. */
    private static final String USER_EMAIL = "user@test.com";

    /** Authentication manager. */
    @Autowired
    private AuthenticationManager authMgr;

    /** Password encoder. */
    @Autowired
    private PasswordEncoder encoder;

    /** Account configuration. */
    @Autowired
    private AccountConfiguration accCfg;

    /** Accounts repository. */
    @Autowired
    private AccountsRepository accountsRepo;

    /**
     * Perform login attempt with wrong credentials
     * Verify the error and error message about wrong credentials.
     * Verify that no. of failed attempts  is equal to 1 and lastFailedLogin is updated
     */
    @Test
    public void shouldSuccessOnAuthenticate() {
        createTestUser();

        authMgr.authenticate(new UsernamePasswordAuthenticationToken(
            USER_EMAIL,
            "password"
        ));

        Account acc = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(0, acc.getFailedLoginAttempts());
        assertEquals(0L, acc.getLastFailedLogin());
    }

    /**
     * Perform login attempt with wrong credentials
     * Immediately try to login with valid credentials
     * Verify the login was successful, no. of failed attempts  and lastFailedLogin are equal to 0
     */
    @Test
    public void shouldFailOnAuthenticate() {
        createTestUser();

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        Account acc = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(1, acc.getFailedLoginAttempts());
        assertNotEquals(0L, acc.getLastFailedLogin());
    }

    /**
     * Perform login attempt with wrong credentials
     * Immediately try to login with valid credentials
     * Verify the login was successful, no. of failed attempts  and lastFailedLogin are equal to 0
     */
    @Test
    public void shouldSuccessAfterFailAuthenticate() {
        createTestUser();

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        authMgr.authenticate(new UsernamePasswordAuthenticationToken(
            USER_EMAIL,
            "password"
        ));

        Account acc = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(0, acc.getFailedLoginAttempts());
        assertEquals(0L, acc.getLastFailedLogin());
    }

    /**
     * Perform login attempt with wrong credentials
     * Immediately try to login with wrong credentials
     * Immediately try to login with valid credentials
     * Verify the error and error message about timeout, no. of failed attempts is 3  and lastFailedLogin is updated
     * Verify the User is revoked
     */
    @Test
    public void shouldLockWithAttemptTooSoon() {
        createTestUser();

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "password"
            ));

            return null;
        }, LockedException.class, "Account is currently locked. Try again later");

        Account acc = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(3, acc.getFailedLoginAttempts());
        assertNotEquals(0L, acc.getLastFailedLogin());
    }

    /**
     * Perform login attempt with wrong credentials
     * Immediately try to login with wrong credentials
     * Verify the error and error message about wrong credentials.
     * Verify that no. of failed attempts  is equal to 2 and lastFailedLogin is updated
     * Wait for a cooldown period
     * Try to login with valid credentials
     * Verify the login was successful, no. of failed attempts  and lastFailedLogin are equal to 0
     */
    @Test
    public void shouldSuccessfullyAuthenticateAfterCooldown() throws InterruptedException {
        createTestUser();

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        Account acc = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(2, acc.getFailedLoginAttempts());
        assertNotEquals(0L, acc.getLastFailedLogin());

        Thread.sleep((long)Math.pow(accCfg.getAuthentication().getInterval(), Math.log(acc.getFailedLoginAttempts())));

        authMgr.authenticate(new UsernamePasswordAuthenticationToken(
            USER_EMAIL,
            "password"
        ));

        acc = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(0, acc.getFailedLoginAttempts());
        assertEquals(0L, acc.getLastFailedLogin());
    }

    /**
     * Perform login attempt with wrong credentials
     * Immediately try to login with wrong credentials
     * Immediately try to login with wrong credentials
     * Verify the User is revoked
     * Verify the error and error message about timeout, no. of failed attempts is 3  and lastFailedLogin is updated
     * Immediately try to login with valid credentials
     * Verify the error, no. of failed attempts and lastFailedLogin
     * Wait for cooldown timeout
     * Try to login with valid credentials
     * Verify the error, no. of failed attempts and lastFailedLogin
     */
    @Test
    public void shouldLockWithTooManyAttempts() throws InterruptedException {
        createTestUser();

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "wrong_password"
            ));

            return null;
        }, BadCredentialsException.class, messages.getMessage("AbstractUserDetailsAuthenticationProvider.badCredentials"));

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "password"
            ));

            return null;
        }, LockedException.class, "Account is currently locked. Try again later");

        Account acc = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(3, acc.getFailedLoginAttempts());
        assertNotEquals(0L, acc.getLastFailedLogin());

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "password"
            ));

            return null;
        }, LockedException.class, "Account locked due to too many failed login attempts");

        Account acc1 = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(4, acc1.getFailedLoginAttempts());
        assertTrue(acc1.getLastFailedLogin() >= acc.getLastFailedLogin());

        Thread.sleep((long)Math.pow(accCfg.getAuthentication().getInterval(), Math.log(acc.getFailedLoginAttempts())));

        GridTestUtils.assertThrows(null, () -> {
            authMgr.authenticate(new UsernamePasswordAuthenticationToken(
                USER_EMAIL,
                "password"
            ));

            return null;
        }, LockedException.class, "Account locked due to too many failed login attempts");

        Account acc2 = accountsRepo.getByEmail(USER_EMAIL);

        assertEquals(5, acc2.getFailedLoginAttempts());
        assertTrue(acc2.getLastFailedLogin() > acc1.getLastFailedLogin());
    }

    /**
     * Create test user.
     */
    private void createTestUser() {
        Account acc = new Account();

        acc.setId(UUID.randomUUID());
        acc.setToken(UUID.randomUUID().toString());
        acc.setEmail(USER_EMAIL);
        acc.setPassword(encoder.encode("password"));

        accountsRepo.save(acc);
    }
}
