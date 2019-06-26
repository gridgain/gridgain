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

import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.Event;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.apache.ignite.console.services.NotificationService;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import static org.apache.ignite.console.event.Event.Type.ACCOUNT_CREATE;
import static org.apache.ignite.console.event.Event.Type.ACCOUNT_CREATE_BY_ADMIN;
import static org.apache.ignite.console.event.Event.Type.ACCOUNT_DELETE;
import static org.apache.ignite.console.event.Event.Type.PASSWORD_CHANGED;
import static org.apache.ignite.console.event.Event.Type.PASSWORD_RESET;
import static org.apache.ignite.console.event.Event.Type.RESET_ACTIVATION_TOKEN;


/**
 * Notification event listener
 */
@Component
public class NotificationEventListener {
    /** Notification server. */
    private NotificationService notificationSrv;

    /**
     * @param notificationSrv Notification server.
     */
    public NotificationEventListener(NotificationService notificationSrv) {
        this.notificationSrv = notificationSrv;
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onUserCreateByAdminEvent(Event<Account> evt) {
        if (evt.getType() == ACCOUNT_CREATE_BY_ADMIN)
            notificationSrv.sendEmail(NotificationDescriptor.ADMIN_WELCOME_LETTER, evt.getPayload());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onUserCreateEvent(Event<Account> evt) {
        if (evt.getType() == ACCOUNT_CREATE)
            notificationSrv.sendEmail(NotificationDescriptor.WELCOME_LETTER, evt.getPayload());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onUserDeleteEvent(Event<Account> evt) {
        if (evt.getType() == ACCOUNT_DELETE)
            notificationSrv.sendEmail(NotificationDescriptor.ACCOUNT_DELETED, evt.getPayload());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onPasswordResetEvent(Event<Account> evt) {
        if (evt.getType() == PASSWORD_RESET)
            notificationSrv.sendEmail(NotificationDescriptor.PASSWORD_RESET, evt.getPayload());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onPasswordChangedEvent(Event<Account> evt) {
        if (evt.getType() == PASSWORD_CHANGED)
            notificationSrv.sendEmail(NotificationDescriptor.PASSWORD_CHANGED, evt.getPayload());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onResetActivationTokenEvent(Event<Account> evt) {
        if (evt.getType() == RESET_ACTIVATION_TOKEN)
            notificationSrv.sendEmail(NotificationDescriptor.ACTIVATION_LINK, evt.getPayload());
    }
}
