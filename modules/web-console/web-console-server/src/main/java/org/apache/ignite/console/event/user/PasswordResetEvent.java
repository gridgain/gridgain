package org.apache.ignite.console.event.user;

import org.apache.ignite.console.dto.Account;

public class PasswordResetEvent extends UserEvent {
    public PasswordResetEvent(Account user) {
        super(user);
    }
}
