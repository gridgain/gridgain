package org.apache.ignite.console.event.user;

import org.apache.ignite.console.dto.Account;
import org.springframework.context.ApplicationEvent;

public abstract class UserEvent extends ApplicationEvent {

    private Account user;

    public UserEvent(Account user) {
        super(new Object());

        this.user = user;
    }

    public Account getUser() {
        return user;
    }
}
