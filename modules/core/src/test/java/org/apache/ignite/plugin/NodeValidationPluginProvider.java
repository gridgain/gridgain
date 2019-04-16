/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.plugin;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Validates node on join, it requires nodes to provide token that matches configured on primary node.
 */
public class NodeValidationPluginProvider implements PluginProvider, IgnitePlugin {
    /** */
    private NodeValidationPluginConfiguration pluginConfiguration;

    /** */
    private static volatile boolean enabled;

    /** */
    public static boolean isEnabled() {
        return enabled;
    }

    /** */
    public static void setEnabled(boolean enabled) {
        NodeValidationPluginProvider.enabled = enabled;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "NodeValidationPluginProvider";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        if (!enabled)
            return;

        IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

        if (igniteCfg.getPluginConfigurations() != null) {
            for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                if (pluginCfg instanceof NodeValidationPluginConfiguration) {
                    pluginConfiguration = (NodeValidationPluginConfiguration)pluginCfg;

                    break;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        //no-op
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        //no-op
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        //no-op
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        //no-op
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        if (!enabled)
            return null;

        MyDiscoData data = new MyDiscoData(pluginConfiguration.getToken());

        return data;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        if (!enabled)
            return;
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node, Serializable serializable) {
        if (!enabled)
            return;

        MyDiscoData newNodeDiscoData = serializable instanceof MyDiscoData ? (MyDiscoData)serializable : null;

        if (newNodeDiscoData == null || !newNodeDiscoData.getToken().equals(pluginConfiguration.getToken())) {
            String msg = newNodeDiscoData == null ? "no token provided" : "bad token provided: " + newNodeDiscoData.getToken();

            throw new PluginValidationException(msg, msg, node.id());
        }
    }

    /**
     *
     */
    private static class MyDiscoData implements Serializable {
        /**  */
        String token;

        /**  */
        MyDiscoData(String token) {
            this.token = token;
        }

        /**  */
        public String getToken() {
            return token;
        }

        /**  */
        @Override public String toString() {
            return "MyDiscoData{" +
                "token='" + token + '\'' +
                '}';
        }
    }

    /**
     *
     */
    public static class NodeValidationPluginConfiguration implements PluginConfiguration {
        /** */
        private final String token;

        /** */
        NodeValidationPluginConfiguration(String token) {
            this.token = token;
        }

        /** */
        public String getToken() {
            return token;
        }
    }
}
