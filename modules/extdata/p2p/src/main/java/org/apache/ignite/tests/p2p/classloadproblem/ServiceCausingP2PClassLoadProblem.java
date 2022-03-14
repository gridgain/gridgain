/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.tests.p2p.classloadproblem;

import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * {@link Service} implementation that breaks p2p class-loading and causes another class to be loaded
 * using p2p when its execute() method is called.
 * <p>
 * NB: Currently, Services do not support p2p class-loading, so this is used just to be sure.
 */
public class ServiceCausingP2PClassLoadProblem implements Service {
    /***/
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        P2PClassLoadingProblems.triggerP2PClassLoadingProblem(getClass(), ignite);
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) {
        // no-op
    }
}
