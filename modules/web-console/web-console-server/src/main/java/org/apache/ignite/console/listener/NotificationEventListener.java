package org.apache.ignite.console.listener;

import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.event.user.*;
import org.apache.ignite.console.notification.NotificationDescriptor;
import org.apache.ignite.console.services.NotificationService;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationEventListener {

    private NotificationService notificationSrv;

    public NotificationEventListener(NotificationService notificationSrv) {
        this.notificationSrv = notificationSrv;
    }

    @EventListener public void onUserCreateEvent(UserCreateEvent event) {
        Account user = event.getUser();

        // TODO: How we can distinct this cases
        if (false) {
            notificationSrv.sendEmail(NotificationDescriptor.ADMIN_WELCOME_LETTER, user);
        } else {
            notificationSrv.sendEmail(NotificationDescriptor.WELCOME_LETTER, user);
        }
    }

    @EventListener public void onUserDeleteEvent(UserDeleteEvent event) {
        notificationSrv.sendEmail(NotificationDescriptor.ACCOUNT_DELETED, event.getUser());
    }

    @EventListener public void onPasswordResetEvent(PasswordResetEvent event) {
        notificationSrv.sendEmail(NotificationDescriptor.PASSWORD_RESET, event.getUser());
    }

    @EventListener public void onPasswordChangedEvent(PasswordChangedEvent event) {
        notificationSrv.sendEmail(NotificationDescriptor.PASSWORD_CHANGED, event.getUser());
    }

    @EventListener public void onResetActivationTokenEvent(ResetActivationTokenEvent event) {
        notificationSrv.sendEmail(NotificationDescriptor.ACTIVATION_LINK, event.getUser());
    }
}
