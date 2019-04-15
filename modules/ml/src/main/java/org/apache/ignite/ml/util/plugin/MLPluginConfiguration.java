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

package org.apache.ignite.ml.util.plugin;

import org.apache.ignite.plugin.PluginConfiguration;

/**
 * Configuration of ML plugin that defines which ML inference services should be start up on Ignite startup.
 */
public class MLPluginConfiguration implements PluginConfiguration {
    /** Model storage should be created on startup. */
    private boolean withMdlStorage;

    /** Model descriptor storage should be created on startup. */
    private boolean withMdlDescStorage;

    /** Number of backups in model storage cache. */
    private Integer mdlStorageBackups;

    /** Number of backups in model descriptor storage cache. */
    private Integer mdlDescStorageBackups;

    /** */
    public boolean isWithMdlStorage() {
        return withMdlStorage;
    }

    /** */
    public void setWithMdlStorage(boolean withMdlStorage) {
        this.withMdlStorage = withMdlStorage;
    }

    /** */
    public boolean isWithMdlDescStorage() {
        return withMdlDescStorage;
    }

    /** */
    public void setWithMdlDescStorage(boolean withMdlDescStorage) {
        this.withMdlDescStorage = withMdlDescStorage;
    }

    /** */
    public Integer getMdlStorageBackups() {
        return mdlStorageBackups;
    }

    /** */
    public void setMdlStorageBackups(Integer mdlStorageBackups) {
        this.mdlStorageBackups = mdlStorageBackups;
    }

    /** */
    public Integer getMdlDescStorageBackups() {
        return mdlDescStorageBackups;
    }

    /** */
    public void setMdlDescStorageBackups(Integer mdlDescStorageBackups) {
        this.mdlDescStorageBackups = mdlDescStorageBackups;
    }
}
