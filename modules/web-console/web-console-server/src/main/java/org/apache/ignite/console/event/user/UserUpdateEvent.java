package org.apache.ignite.console.event.user;

import org.apache.ignite.console.dto.Account;

public class UserUpdateEvent extends UserEvent {
    public UserUpdateEvent(Account user) {
        super(user);
    }
}
