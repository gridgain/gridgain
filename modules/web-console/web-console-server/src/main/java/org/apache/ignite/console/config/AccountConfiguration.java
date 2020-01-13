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

import org.apache.ignite.console.services.IMailService;
import org.apache.ignite.console.services.NoopMailService;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Account configuration.
 */
@Configuration
@ConfigurationProperties("account")
public class AccountConfiguration  implements InitializingBean {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AccountConfiguration.class);

    /** Is noop mail service. */
    private final boolean isNoopMailSrv;

    /** Activation. */
    private Activation activation = new Activation();

    /** Authentication. */
    private Authentication authentication = new Authentication();

    /**
     * @param srv Mail sending service.
     */
    public AccountConfiguration(IMailService srv) {
        isNoopMailSrv = srv instanceof NoopMailService;
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() {
        if (activation.enabled && isNoopMailSrv) {
            activation.enabled = false;

            log.warn("Mail server settings are required for account confirmation.");
        }
    }

    /**
     * @return Activation configuration.
     */
    public Activation getActivation() {
        return activation;
    }

    /**
     * @param activation Activation configuration.
     */
    public void setActivation(Activation activation) {
        this.activation = activation;
    }

    /**
     * @return Authentication configuration.
     */
    public Authentication getAuthentication() {
        return authentication;
    }

    /**
     * @param authentication Authentication configuration.
     */
    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AccountConfiguration.class, this);
    }

    /**
     * Account activation configuration.
     */
    public static class Activation {
        /** By default activation link will be available for 24 hours. */
        private static final long DFLT_TIMEOUT = 24 * 60 * 60 * 1000;

        /** By default activation send email throttle is 3 minutes. */
        private static final long DFLT_SEND_TIMEOUT = 3 * 60 * 1000;

        /** Activation link life time. */
        private long timeout = DFLT_TIMEOUT;

        /** Activation send email throttle. */
        private long sndTimeout = DFLT_SEND_TIMEOUT;

        /** Whether account should be activated by e-mail confirmation. */
        private boolean enabled;

        /**
         * @return {@code true} if new accounts should be activated via e-mail confirmation.
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * @param enabled Accounts activation required flag.
         * @return {@code this} for chaining.
         */
        public Activation setEnabled(boolean enabled) {
            this.enabled = enabled;

            return this;
        }

        /**
         * @return Activation link life time.
         */
        public long getTimeout() {
            return timeout;
        }

        /**
         * @param timeout Activation link life time.
         * @return {@code this} for chaining.
         */
        public Activation setTimeout(long timeout) {
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
        public Activation setSendTimeout(long sndTimeout) {
            this.sndTimeout = sndTimeout;

            return this;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Activation.class, this);
        }
    }

    /**
     * Account authentication configuration.
     */
    public static class Authentication {
        /** */
        private static final long DFLT_INTERVAL = 100L;

        /** */
        private static final long DFLT_MAX_INTERVAL = MINUTES.toMillis(5);

        /** Specifies the maximum number of failed attempts allowed before preventing login. */
        private int maxAttempts = Integer.MAX_VALUE;

        /**
         * Specifies the interval in milliseconds between login attempts, which increases exponentially based on the
         * number of failed attempts.
         */
        private long interval = DFLT_INTERVAL;

        /** Specifies the maximum amount of time an account can be locked. */
        private long maxInterval = DFLT_MAX_INTERVAL;

        /**
         * @return Specifies the maximum number of failed attempts allowed before preventing login.
         */
        public int getMaxAttempts() {
            return maxAttempts;
        }

        /**
         * @param maxAttempts New specifies the maximum number of failed attempts allowed before preventing login.
         */
        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        /**
         * @return Specifies the interval in milliseconds between login attempts, which increases exponentially based on
         * the number of failed attempts.
         */
        public long getInterval() {
            return interval;
        }

        /**
         * @param interval New specifies the interval in milliseconds between login attempts, which increases
         * exponentially based on the number of failed attempts.
         */
        public void setInterval(long interval) {
            this.interval = interval;
        }

        /**
         * @return Specifies the maximum amount of time an account can be locked.
         */
        public long getMaxInterval() {
            return maxInterval;
        }

        /**
         * @param maxInterval New specifies the maximum amount of time an account can be locked.
         */
        public void setMaxInterval(long maxInterval) {
            this.maxInterval = maxInterval;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Authentication.class, this);
        }
    }
}
