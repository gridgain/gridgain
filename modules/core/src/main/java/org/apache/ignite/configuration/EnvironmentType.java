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

package org.apache.ignite.configuration;

import org.apache.ignite.lang.IgniteExperimental;

/**
 * This enum represents general hint for Ignite about its environment: stand-alone or virtualized.
 *
 * Virtualized deployments may come with limitations like networks hidden behind NATs or may
 * bring external automated management as Kubernetes does.
 * However from inside virtualized environment looks exactly the same as a stand-alone one and
 * Ignite node cannot detect type of the environment and adapt its algorithms to these factors.
 *
 * {@link EnvironmentType} enum enables user to give a hint to started Ignite node so it is able to function efficiently.
 */
@IgniteExperimental
public enum EnvironmentType {
    /** Default value. */
    STANDALONE,

    /** */
    VIRTUALIZED;
}
