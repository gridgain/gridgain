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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformExternalMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Base interop bootstrap implementation.
 */
public abstract class PlatformAbstractBootstrap implements PlatformBootstrap {
    /** Use logger flag. */
    protected boolean useLogger;

    /** {@inheritDoc} */
    @Override public PlatformProcessor start(IgniteConfiguration cfg, @Nullable GridSpringResourceContext springCtx,
        long envPtr) {

        IgniteConfiguration cfg0 = closure(envPtr).apply(cfg);

        try {
            IgniteEx node = (IgniteEx)IgnitionEx.start(cfg0, springCtx);

            return node.context().platform();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void init(long dataPtr) {
        final PlatformInputStream input = new PlatformExternalMemory(null, dataPtr).input();

        Ignition.setClientMode(input.readBoolean());

        processInput(input);
    }

    /**
     * Get configuration transformer closure.
     *
     * @param envPtr Environment pointer.
     * @return Closure.
     */
    protected abstract IgniteClosure<IgniteConfiguration, IgniteConfiguration> closure(long envPtr);

    /**
     * Processes any additional input data.
     *
     * @param input Input stream.
     */
    protected void processInput(PlatformInputStream input) {
        useLogger = input.readBoolean();
    }
}
