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

package org.apache.ignite.mxbean;

import java.util.List;
import org.apache.ignite.internal.processors.ru.RollingUpgradeProcessor;

/**
 * This interface defines JMX view on {@link RollingUpgradeProcessor}.
 */
@MXBeanDescription("MBean that provides access to rolling upgrade.")
public interface RollingUpgradeMXBean {
    /**
     * Returns {@code true} if Rolling Upgrade is enabled and is in progress.
     *
     * @return {@code true} if Rolling Upgrade is enabled and is in progress.
     */
    @MXBeanDescription("State of rolling upgrade (enabled/disabled).")
    boolean isEnabled();

    /**
     * Returns the version that is used as starting point for Rolling Upgrade.
     *
     * @return String representation of initial version.
     */
    @MXBeanDescription("The initial cluster version.")
    String getInitialVersion();

    /**
     * Returns a list of alive nodes in the cluster that are not updated yet.
     *
     * @return List of nodes that are not updated.
     */
    @MXBeanDescription("List of alive nodes in the cluster that are not updated yet.")
    List<String> getInitialNodes();

    /**
     * Returns target cluster version.
     *
     * @return String representation of target cluster version.
     */
    @MXBeanDescription("The target cluster version.")
    String getUpdateVersion();

    /**
     * Returns a list of alive nodes in the cluster that are updated.
     *
     * @return List of alive nodes in the cluster that are updated.
     */
    @MXBeanDescription("List of alive nodes in the cluster that are updated.")
    List<String> getUpdatedNodes();

    /**
     * Returns a list of features that is supported by the cluster.
     *
     * @return Feature set that is supported by the cluster.
     */
    @MXBeanDescription("Feature set that is supported by nodes.")
    List<String> getSupportedFeatures();

    /**
     * Allows to enable or disable rolling upgrade mode.
     *
     * @param enable {@code true} if rolling upgrade mode should be enabled.
     */
    @MXBeanDescription("Set rolling upgrade mode value.")
    @MXBeanParametersNames("enable")
    @MXBeanParametersDescriptions("Rolling upgrade mode.")
    void changeMode(boolean enable);
}
