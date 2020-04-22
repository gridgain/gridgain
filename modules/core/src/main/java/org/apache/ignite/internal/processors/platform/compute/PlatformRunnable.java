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

package org.apache.ignite.internal.processors.platform.compute;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * Runnable implementation that delegates to native platform.
 */
public class PlatformRunnable extends PlatformAbstractFunc implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     *
     * @param func Platform func.
     * @param ptr Handle for local execution.
     */
    public PlatformRunnable(Object func, long ptr) {
        super(func, ptr);
    }

    /** <inheritdoc /> */
    @Override protected void platformCallback(PlatformCallbackGateway gate, long memPtr) {
        gate.computeActionExecute(memPtr);
    }

    /** <inheritdoc /> */
    @Override public void run() {
        try {
            invoke();
        } catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}
