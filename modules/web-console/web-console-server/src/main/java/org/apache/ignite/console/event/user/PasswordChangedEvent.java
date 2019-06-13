package org.apache.ignite.console.event.user;

import org.apache.ignite.console.dto.Account;

public class PasswordChangedEvent extends UserEvent {
    public PasswordChangedEvent(Account user) {
        super(user);
    }
}
