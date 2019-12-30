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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Account authentication configuration.
 */
@Configuration
@ConfigurationProperties("account.authentication")
public class AccountAuthenticationConfiguration {
    /** */
    private static final long DFLT_INTERVAL = 100L;

    /** */
    private static final long DFLT_MAX_INTERVAL = MINUTES.toMillis(5);

    /** Specifies the maximum number of failed attempts allowed before preventing login. */
    private long maxAttempts = Long.MAX_VALUE;

    /**
     * Specifies the interval in milliseconds between login attempts, which increases exponentially based on the number
     * of failed attempts.
     */
    private long interval = DFLT_INTERVAL;

    /** Specifies the maximum amount of time an account can be locked. */
    private long maxInterval = DFLT_MAX_INTERVAL;

    /**
     * @return Specifies the maximum number of failed attempts allowed before preventing login.
     */
    public long getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * @param maxAttempts New specifies the maximum number of failed attempts allowed before preventing login.
     */
    public void setMaxAttempts(long maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    /**
     * @return Specifies the interval in milliseconds between login attempts, which increases exponentially based on the
     * number of failed attempts.
     */
    public long getInterval() {
        return interval;
    }

    /**
     * @param interval New specifies the interval in milliseconds between login attempts, which increases exponentially
     * based on the number of failed attempts.
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
        return S.toString(AccountAuthenticationConfiguration.class, this);
    }
}
