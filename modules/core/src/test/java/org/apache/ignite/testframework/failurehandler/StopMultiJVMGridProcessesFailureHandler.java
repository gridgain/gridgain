/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.testframework.failurehandler;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * Failure Handler intended to stops all remote JVM processes. Beware! Can be used only for multi JVM test purposes with caution.
 */
public class StopMultiJVMGridProcessesFailureHandler extends AbstractFailureHandler {

    /** {@inheritDoc} */
    @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
        IgniteLogger log = ignite.log();

        try {
            U.error(log, "Do stop grid processes");
            IgniteProcessProxy.killAll();
        }
        catch (Exception exc) {
            U.error(log, "Stop grid processes error");
        }

        return true;
    }
}
