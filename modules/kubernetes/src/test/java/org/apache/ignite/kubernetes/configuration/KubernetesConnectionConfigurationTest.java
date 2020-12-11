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

package org.apache.ignite.kubernetes.configuration;

import org.apache.ignite.spi.IgniteSpiException;
import org.junit.Before;
import org.junit.Test;

/** Class checks required fields for {@link KubernetesConnectionConfiguration} */
public class KubernetesConnectionConfigurationTest {
    /** */
    private KubernetesConnectionConfiguration cfg;

    /** Fill all required fields. */
    @Before
    public void setup() {
        cfg = new KubernetesConnectionConfiguration();

        cfg.setAccountToken("accountToken");
        cfg.setMasterUrl("masterUrl");
        cfg.setNamespace("namespace");
        cfg.setServiceName("serviceName");
    }

    /** */
    @Test
    public void smoketest() {
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testAccountNameIsRequired() {
        cfg.setAccountToken(null);
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testMasterUrlIsRequired() {
        cfg.setMasterUrl(null);
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testNamespaceIsRequired() {
        cfg.setNamespace(null);
        cfg.verify();
    }

    /** */
    @Test(expected = IgniteSpiException.class)
    public void testServiceNameIsRequired() {
        cfg.setServiceName("");
        cfg.verify();
    }
}
