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
 * Configuration of Web Console.
 */
public class WebConsoleConfiguration {
    /** */
    private static final int DFLT_PORT = 3000;

    /** Path to static content. */
    private String webRoot;

    /** Web Console port. */
    private int port = DFLT_PORT;

    /** Accounts configuration. */
    private ActivationConfiguration accountCfg;

    /** SSL configuration. */
    private SslConfiguration sslCfg;

    /**
     * Empty constructor.
     */
    public WebConsoleConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cc Configuration to copy.
     */
    public WebConsoleConfiguration(WebConsoleConfiguration cc) {
       webRoot = cc.getWebRoot();
       port = cc.getPort();
       accountCfg = cc.getAccountConfiguration();
       sslCfg = cc.getSslConfiguration();
    }

    /**
     * @return Path to static content
     */
    public String getWebRoot() {
        return webRoot;
    }

    /**
     * @param webRoot Path to static content.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setWebRoot(String webRoot) {
        this.webRoot = webRoot;

        return this;
    }

    /**
     * @return Port to listen.
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port Port to listen.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setPort(int port) {
        this.port = port;

        return this;
    }

    /**
     * @return Account configuration.
     */
    public ActivationConfiguration getAccountConfiguration() {
        return accountCfg;
    }

    /**
     * @param activationCfg Account configuration.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setAccountConfiguration(ActivationConfiguration activationCfg) {
        this.accountCfg = activationCfg;

        return this;
    }

    /**
     * @return SSL configuration.
     */
    public SslConfiguration getSslConfiguration() {
        return sslCfg;
    }

    /**
     * @param sslCfg SSL configuration.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setSslConfiguration(SslConfiguration sslCfg) {
        this.sslCfg = sslCfg;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebConsoleConfiguration.class, this);
    }
}
