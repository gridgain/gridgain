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

package org.gridgain.service.config;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;

/**
 * Cluster configuration.
 */
@JsonIgnoreProperties(
    ignoreUnknown = true,
    allowSetters = true,
    value = {
        "marshaller",
        "mbeanServer",
        "cacheConfiguration",
        "dataStorageConfiguration",
        "gridLogger",
        "loadBalancingSpi",
        "indexingSpi",
        "encryptionSpi",
        "metricExporterSpi",
        "tracingSpi",
        "eventStorageSpi",
        "discoverySpi",
        "communicationSpi",
        "checkpointSpi",
        "failoverSpi",
        "collisionSpi"
    }
)
public class ClusterConfiguration extends IgniteConfiguration {
    /** Local event listeners. */
    private List<ClusterLocalEventListener> lsnrs;

    /** Deployment SPI. */
    private ClusterDeploymentSpi deploySpi;

    /**
     * Creates valid grid configuration with all default values.
     */
    public ClusterConfiguration() {
        super();
    }

    /**
     * Creates grid configuration by coping all configuration properties from
     * given configuration.
     *
     * @param cfg Grid configuration to copy from.
     */
    public ClusterConfiguration(IgniteConfiguration cfg) {
        super(cfg);
    }

    /**
     * Should return fully configured deployment SPI implementation. If not provided, {@link LocalDeploymentSpi} will be
     * used.
     *
     * @return Grid deployment SPI implementation or {@code null} to use default implementation.
     */
    @JsonGetter("deploymentSpi")
    public ClusterDeploymentSpi getDeploymentSpi0() {
        return deploySpi;
    }

    /**
     * Sets fully configured instance of {@link DeploymentSpi}.
     *
     * @param deploySpi Fully configured instance of {@link DeploymentSpi}.
     * @return {@code this} for chaining.
     * @see IgniteConfiguration#getDeploymentSpi()
     */
    @JsonSetter("deploymentSpi")
    public ClusterConfiguration setDeploymentSpi0(ClusterDeploymentSpi deploySpi) {
        this.deploySpi = deploySpi;

        return this;
    }

    @JsonGetter("localEventListeners")
    public List<ClusterLocalEventListener> getLocalEventListeners0() {
        return lsnrs;
    }

    @JsonSetter("localEventListeners")
    public ClusterConfiguration setLocalEventListeners0(List<ClusterLocalEventListener> lsnrs) {
        this.lsnrs = lsnrs;

        return this;
    }

    public static class ClusterDeploymentSpi {
        /** Kind. */
        private String kind;
        
        /** Local. */
        private LocalDeploymentSpi local;

        /**
         * @return value of kind
         */
        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        /**
         * @return value of Local
         */
        public LocalDeploymentSpi getLocal() {
            return local;
        }

        public void setLocal(LocalDeploymentSpi local) {
            this.local = local;
        }
    }

    /**
     * 
     */
    public static class ClusterLocalEventListener {
        /** Class name. */
        private String clsName;
        
        /** Event types. */
        private int[] evtTypes;

        /**
         * @return value of className
         */
        public String getClassName() {
            return clsName;
        }

        /**
         * @param clsName Class name.
         */
        public ClusterLocalEventListener setClassName(String clsName) {
            this.clsName = clsName;

            return this;
        }

        /**
         * @return value of eventTypes
         */
        public int[] getEventTypes() {
            return evtTypes;
        }

        /**
         * @param evtTypes Event types.
         */
        public ClusterLocalEventListener setEventTypes(int[] evtTypes) {
            this.evtTypes = evtTypes;

            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ClusterLocalEventListener lsnr = (ClusterLocalEventListener)o;
            
            return clsName.equals(lsnr.clsName) &&
                Arrays.equals(evtTypes, lsnr.evtTypes);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = Objects.hash(clsName);

            res = 31 * res + Arrays.hashCode(evtTypes);

            return res;
        }
    }
}
