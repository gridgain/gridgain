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

package org.apache.ignite.internal.processors.service;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Actions of change service state to be processed in the service deployment process.
 */
public class ServiceDeploymentActions {
    /** Whenever it's necessary to deactivate service processor. */
    private boolean deactivate;

    /** Services info to deploy. */
    private Map<IgniteUuid, ServiceInfo> servicesToDeploy;

    /** Services info to undeploy. */
    private Map<IgniteUuid, ServiceInfo> servicesToUndeploy;

    /** Services deployment topologies. */
    private Map<IgniteUuid, Map<UUID, Integer>> depTops;

    /** Services deployment errors. */
    private Map<IgniteUuid, Collection<byte[]>> depErrors;

    /**
     * @param servicesToDeploy Services info to deploy.
     */
    public void servicesToDeploy(@NotNull Map<IgniteUuid, ServiceInfo> servicesToDeploy) {
        this.servicesToDeploy = servicesToDeploy;
    }

    /**
     * @return Services info to deploy.
     */
    @NotNull public Map<IgniteUuid, ServiceInfo> servicesToDeploy() {
        return servicesToDeploy != null ? servicesToDeploy : Collections.emptyMap();
    }

    /**
     * @param servicesToUndeploy Services info to undeploy.
     */
    public void servicesToUndeploy(@NotNull Map<IgniteUuid, ServiceInfo> servicesToUndeploy) {
        this.servicesToUndeploy = servicesToUndeploy;
    }

    /**
     * @return Services info to undeploy.
     */
    @NotNull public Map<IgniteUuid, ServiceInfo> servicesToUndeploy() {
        return servicesToUndeploy != null ? servicesToUndeploy : Collections.emptyMap();
    }

    /**
     * @param deactivate Whenever it's necessary to deactivate service processor.
     */
    public void deactivate(boolean deactivate) {
        this.deactivate = deactivate;
    }

    /**
     * @return Whenever it's necessary to deactivate service processor.
     */
    public boolean deactivate() {
        return deactivate;
    }

    /**
     * @return Deployment topologies.
     */
    @NotNull public Map<IgniteUuid, Map<UUID, Integer>> deploymentTopologies() {
        return depTops != null ? depTops : Collections.emptyMap();
    }

    /**
     * @param depTops Deployment topologies.
     */
    public void deploymentTopologies(@NotNull Map<IgniteUuid, Map<UUID, Integer>> depTops) {
        this.depTops = depTops;
    }

    /**
     * @return Deployment errors.
     */
    @NotNull public Map<IgniteUuid, Collection<byte[]>> deploymentErrors() {
        return depErrors != null ? depErrors : Collections.emptyMap();
    }

    /**
     * @param depErrors Deployment errors.
     */
    public void deploymentErrors(@NotNull Map<IgniteUuid, Collection<byte[]>> depErrors) {
        this.depErrors = depErrors;
    }
}
