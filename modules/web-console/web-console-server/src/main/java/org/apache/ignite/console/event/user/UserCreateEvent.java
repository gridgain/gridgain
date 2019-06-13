package org.apache.ignite.console.event.user;

import org.apache.ignite.console.dto.Account;

public class UserCreateEvent extends UserEvent {
    public UserCreateEvent(Account user) {
        super(user);
    }
}
