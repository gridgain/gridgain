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

package org.apache.ignite.agent;

import org.junit.After;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;

import static org.testcontainers.containers.BindMode.READ_ONLY;

/**
 * Agent with proxt abstract test.
 */
public abstract class AgentWithProxyAbstractTest extends AgentCommonAbstractTest {
    /**
     * Clear proxy properties.
     */
    @After
    public void cleanupProperties() {
        System.clearProperty("http.proxyHost");
        System.clearProperty("http.proxyPort");

        System.clearProperty("http.proxyUsername");
        System.clearProperty("http.proxyPassword");
    }

    /**
     * Start HTTP proxy.
     */
    public GenericContainer startProxy() {
        Testcontainers.exposeHostPorts(port);

        GenericContainer proxy = new GenericContainer<>("salrashid123/squidproxy")
            .withClasspathResourceMapping("proxy/squid.conf", "/etc/squid/squid.conf", READ_ONLY)
            .withCommand("/apps/squid/sbin/squid -NsY -f /etc/squid/squid.conf")
            .withExposedPorts(3128);

        proxy.start();

        return proxy;
    }

    /**
     * Start HTTP proxy with authorization.
     */
    public GenericContainer startProxyWithCreds() {
        GenericContainer proxy = new GenericContainer<>("salrashid123/squidproxy")
            .withClasspathResourceMapping("proxy/squid-auth.conf", "/etc/squid/squid.conf", READ_ONLY)
            .withClasspathResourceMapping("proxy/passwords", "/etc/squid3/passwords", READ_ONLY)
            .withCommand("/apps/squid/sbin/squid -NsY -f /etc/squid/squid.conf")
            .withExposedPorts(3128);

        proxy.start();

        return proxy;
    }

    /**
     * Start HTTPS proxy.
     */
    public GenericContainer startHttpsProxy() {
        GenericContainer proxy = new GenericContainer<>("salrashid123/squidproxy")
            .withClasspathResourceMapping("proxy/squid-https.conf", "/etc/squid/squid.conf", READ_ONLY)
            .withClasspathResourceMapping("ssl/ca.crt", "/etc/squid3/ca.crt", READ_ONLY)
            .withClasspathResourceMapping("ssl/ca.key", "/etc/squid3/ca.key", READ_ONLY)
            .withCommand("/apps/squid/sbin/squid -NsY -f /etc/squid/squid.conf")
            .withExposedPorts(3128);

        proxy.start();

        return proxy;
    }
}
