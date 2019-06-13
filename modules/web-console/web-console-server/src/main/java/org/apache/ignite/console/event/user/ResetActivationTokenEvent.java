package org.apache.ignite.console.event.user;

import org.apache.ignite.console.dto.Account;

public class ResetActivationTokenEvent extends UserEvent {
    public ResetActivationTokenEvent(Account user) {
        super(user);
    }
}
