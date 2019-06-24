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

import org.apache.ignite.console.TestConfiguration;
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.config.SignUpConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.event.user.*;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.security.MissingConfirmRegistrationException;
import org.apache.ignite.console.web.socket.WebSocketsManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Account service test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class})
public class AccountServiceTest {
    /** Account repository. */
    @Mock
    private AccountsRepository accountsRepo;

    /** Event publisher. */
    @Mock
    private EventPublisher evtPublisher;

    /** Tx manager. */
    @Autowired
    private TransactionManager txMgr;

    /** */
    @Before
    public void setup() {
        when(accountsRepo.create(any(Account.class)))
            .thenAnswer(invocation -> {
                Account acc = invocation.getArgumentAt(0, Account.class);

                if ("admin@admin".equalsIgnoreCase(acc.getUsername()))
                    acc.setAdmin(true);

                return acc;
            });
    }

    /** Test sign up logic. */
    @Test
    public void disableSignUp() {
        AccountsService srvc = mockAccountsService(false, false);

        SignUpRequest adminReq = new SignUpRequest();

        adminReq.setEmail("admin@admin");
        adminReq.setPassword("1");

        srvc.register(adminReq);

        GridTestUtils.assertThrows(null, () -> {
            SignUpRequest userReq = new SignUpRequest();

            userReq.setEmail("user@user");
            userReq.setPassword("1");

            srvc.register(userReq);

            return null;
        }, AuthenticationServiceException.class, null);
    }

    /**
     * Should publish {@link org.apache.ignite.console.event.user.ResetActivationTokenEvent}
     */
    @Test
    public void shouldPublishResetActivationTokenEventWhileRegister() {
        AccountsService srvc = mockAccountsService(true, true);

        SignUpRequest userReq = new SignUpRequest();

        userReq.setEmail("user@user");
        userReq.setPassword("1");

        try {
            srvc.register(userReq);
        } catch (MissingConfirmRegistrationException exception) {
            // No-op
        }

        ArgumentCaptor<ResetActivationTokenEvent> captor = ArgumentCaptor.forClass(ResetActivationTokenEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals("user@user", captor.getValue().user().getEmail());
        Assert.assertNotEquals(null, captor.getValue().user().getId());
    }

    /**
     * Should publish {@link org.apache.ignite.console.event.user.ResetActivationTokenEvent}
     */
    @Test
    public void shouldPublishResetActivationTokenEvent() {
        AccountsService srvc = mockAccountsService(true, true);
        when(accountsRepo.getByEmail(anyString()))
                .thenAnswer(invocation -> {
                    Account acc = new Account();
                    acc.setEmail(invocation.getArgumentAt(0, String.class));
                    acc.resetActivationToken();

                    return acc;
                });

        srvc.resetActivationToken("mail@mail");

        ArgumentCaptor<ResetActivationTokenEvent> captor = ArgumentCaptor.forClass(ResetActivationTokenEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals("mail@mail", captor.getValue().user().getEmail());
    }

    /**
     * Should publish {@link org.apache.ignite.console.event.user.UserCreateEvent}
     */
    @Test
    public void shouldPublishUserCreateEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        SignUpRequest userReq = new SignUpRequest();

        userReq.setEmail("user@user");
        userReq.setPassword("1");

        srvc.register(userReq);

        ArgumentCaptor<UserCreateEvent> captor = ArgumentCaptor.forClass(UserCreateEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals("user@user", captor.getValue().user().getEmail());
        Assert.assertNotEquals(null, captor.getValue().user().getId());
    }

    /**
     * Should publish {@link org.apache.ignite.console.event.user.UserUpdateEvent}
     */
    @Test
    public void shouldPublishUserUpdateEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        when(accountsRepo.getById(any(UUID.class)))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setEmail("fake@mail");
                acc.setId(invocation.getArgumentAt(0, UUID.class));
                acc.setToken("token");

                return acc;
            });

        ChangeUserRequest changes = new ChangeUserRequest();
        changes.setEmail("new@mail");

        srvc.save(UUID.randomUUID(), changes);

        ArgumentCaptor<UserUpdateEvent> captor = ArgumentCaptor.forClass(UserUpdateEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals("new@mail", captor.getValue().user().getEmail());
    }

    /**
     * Should publish {@link org.apache.ignite.console.event.user.PasswordResetEvent}
     */
    @Test
    public void shouldPublishPasswordResetEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        when(accountsRepo.getByEmail(anyString()))
            .thenAnswer(invocation -> {
                Account acc = new Account();
                acc.setEmail(invocation.getArgumentAt(0, String.class));

                return acc;
            });

        srvc.forgotPassword("mail@mail");

        ArgumentCaptor<PasswordResetEvent> captor = ArgumentCaptor.forClass(PasswordResetEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals("mail@mail", captor.getValue().user().getEmail());
    }

    /**
     * Should publish {@link org.apache.ignite.console.event.user.PasswordChangedEvent}
     */
    @Test
    public void shouldPublishPasswordChangedEvent() {
        AccountsService srvc = mockAccountsService(true, false);

        when(accountsRepo.getByEmail(anyString()))
                .thenAnswer(invocation -> {
                    Account acc = new Account();
                    acc.setEmail(invocation.getArgumentAt(0, String.class));
                    acc.setResetPasswordToken("token");

                    return acc;
                });

        srvc.resetPasswordByToken("new_mail@mail", "token", "2");

        ArgumentCaptor<PasswordChangedEvent> captor = ArgumentCaptor.forClass(PasswordChangedEvent.class);
        verify(evtPublisher, times(1)).publish(captor.capture());

        Assert.assertEquals("new_mail@mail", captor.getValue().user().getEmail());
    }

    /**
     * @param disableSignUp Disable sign up.
     */
    private AccountsService mockAccountsService(boolean disableSignUp, boolean enableActivation) {
        ActivationConfiguration activationCfg = new ActivationConfiguration(new NoopMailService());
        try {
            activationCfg.afterPropertiesSet();
        } catch (Exception e) {
            // No-op
        }
        activationCfg.setEnabled(enableActivation);

        return new AccountsService(
                new SignUpConfiguration().setEnabled(disableSignUp),
                activationCfg,
                NoOpPasswordEncoder.getInstance(),
                new WebSocketsManager(),
                accountsRepo,
                txMgr,
                evtPublisher
        );
    }
}
