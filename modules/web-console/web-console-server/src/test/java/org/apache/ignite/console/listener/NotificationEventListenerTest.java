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

package org.apache.ignite.console.listener;

import org.apache.ignite.console.TestConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.EventPublisher;
import org.apache.ignite.console.event.user.*;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.apache.ignite.console.services.NotificationService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestConfiguration.class})
public class NotificationEventListenerTest {
    /** */
    @Autowired
    private EventPublisher publisher;

    /** */
    @MockBean
    private NotificationService notificationSrv;

    /**
     * Test welcome letter sending
     */
    @Test
    public void testOnUserCreateEvent() throws InterruptedException {
        Account acc = new Account();
        UserEvent evt = new UserCreateEvent(acc);

        publisher.publish(evt);
        Thread.sleep(1);

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.WELCOME_LETTER, acc);
    }

    /**
     * Test admin welcome letter sending
     */
    @Test
    public void testOnUserCreateByAdminEvent() throws InterruptedException {
        Account acc = new Account();
        UserEvent evt = new UserCreateByAdminEvent(acc);

        publisher.publish(evt);
        Thread.sleep(1);

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.ADMIN_WELCOME_LETTER, acc);
    }

    /**
     * Test account delete letter sending
     */
    @Test
    public void testOnUserDeleteEvent() throws InterruptedException {
        Account acc = new Account();
        UserEvent evt = new UserDeleteEvent(acc);

        publisher.publish(evt);
        Thread.sleep(1);

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.ACCOUNT_DELETED, evt.getUser());
    }

    /**
     * Test password reset letter sending
     */
    @Test
    public void testOnPasswordResetEvent() throws InterruptedException {
        Account acc = new Account();
        UserEvent evt = new PasswordResetEvent(acc);

        publisher.publish(evt);
        Thread.sleep(1);

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.PASSWORD_RESET, evt.getUser());
    }

    /**
     * Test password changed letter sending
     */
    @Test
    public void testOnPasswordChangedEvent() throws InterruptedException {
        Account acc = new Account();
        UserEvent evt = new PasswordChangedEvent(acc);

        publisher.publish(evt);
        Thread.sleep(1);

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.PASSWORD_CHANGED, evt.getUser());
    }

    /**
     * Test reset activation tokent letter sending
     */
    @Test
    public void testOnResetActivationTokenEvent() throws InterruptedException {
        Account acc = new Account();
        UserEvent evt = new ResetActivationTokenEvent(acc);

        publisher.publish(evt);
        Thread.sleep(1);

        verify(notificationSrv, times(1)).sendEmail(NotificationDescriptor.ACTIVATION_LINK, evt.getUser());
    }
}