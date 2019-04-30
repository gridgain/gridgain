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

package org.apache.ignite.console.config;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Account configuration.
 */
public class AccountConfiguration {
    /** By default activation link will be available for 24 hours. */
    private static final long DFLT_TIMEOUT = 24 * 60 * 60 * 1000;

    /** By default activation send email throttle is 3 minutes. */
    private static final long DFLT_SEND_TIMEOUT = 3 * 60 * 1000;

    /** Whether account should be activated by e-mail confirmation. */
    private boolean confirmationRequired;

    /** Activation link life time. */
    private long timeout = DFLT_TIMEOUT;

    /** Activation send email throttle. */
    private long sndTimeout = DFLT_SEND_TIMEOUT;

    /** Flag if sign up disabled and new accounts can be created only by administrator. */
    private boolean disableSignup;

    /**
     * Empty constructor.
     */
    public AccountConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cc Configuration to copy.
     */
    public AccountConfiguration(AccountConfiguration cc) {
        confirmationRequired = cc.isConfirmationRequired();
        timeout = cc.getTimeout();
        sndTimeout = cc.getSendTimeout();
        disableSignup = cc.isDisableSignup();
    }

    /**
     * @return {@code true} if new accounts should be activated via e-mail confirmation.
     */
    public boolean isConfirmationRequired() {
        return confirmationRequired;
    }

    /**
     * @param confirmationRequired Accounts activation required flag.
     * @return {@code this} for chaining.
     */
    public AccountConfiguration setConfirmationRequired(boolean confirmationRequired) {
        this.confirmationRequired = confirmationRequired;

        return this;
    }

    /**
     * @return Activation link life time.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     *
     * @param timeout Activation link life time.
     * @return {@code this} for chaining.
     */
    public AccountConfiguration setTimeout(long timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return Activation send email throttle.
     */
    public long getSendTimeout() {
        return sndTimeout;
    }

    /**
     * @param sndTimeout Activation send email throttle.
     * @return {@code this} for chaining.
     */
    public AccountConfiguration setSendTimeout(long sndTimeout) {
        this.sndTimeout = sndTimeout;

        return this;
    }

    /**
     * @return {@code true} if sign up disabled and new accounts can be created only by administrator.
     */
    public boolean isDisableSignup() {
        return disableSignup;
    }

    /**
     * @param disableSignup {@code true} if signup disabled and new accounts can be created only by administrator.
     * @return {@code this} for chaining.
     */
    public AccountConfiguration setDisableSignup(boolean disableSignup) {
        this.disableSignup = disableSignup;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AccountConfiguration.class, this);
    }
}
