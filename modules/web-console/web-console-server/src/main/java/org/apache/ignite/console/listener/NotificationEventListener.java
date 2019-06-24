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

import org.apache.ignite.console.event.user.*;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.apache.ignite.console.services.NotificationService;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/** */
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
    public void onUserCreateByAdminEvent(UserCreateByAdminEvent evt) {
        notificationSrv.sendEmail(NotificationDescriptor.ADMIN_WELCOME_LETTER, evt.user());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onUserCreateEvent(UserCreateEvent evt) {
        notificationSrv.sendEmail(NotificationDescriptor.WELCOME_LETTER, evt.user());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onUserDeleteEvent(UserDeleteEvent evt) {
        notificationSrv.sendEmail(NotificationDescriptor.ACCOUNT_DELETED, evt.user());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onPasswordResetEvent(PasswordResetEvent evt) {
        notificationSrv.sendEmail(NotificationDescriptor.PASSWORD_RESET, evt.user());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onPasswordChangedEvent(PasswordChangedEvent evt) {
        notificationSrv.sendEmail(NotificationDescriptor.PASSWORD_CHANGED, evt.user());
    }

    /**
     * @param evt Event.
     */
    @EventListener
    public void onResetActivationTokenEvent(ResetActivationTokenEvent evt) {
        notificationSrv.sendEmail(NotificationDescriptor.ACTIVATION_LINK, evt.user());
    }
}
