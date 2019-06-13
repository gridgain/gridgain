package org.apache.ignite.console.event.user;

import org.apache.ignite.console.dto.Account;

public class UserDeleteEvent extends UserEvent {
    public UserDeleteEvent(Account user) {
        super(user);
    }
}
