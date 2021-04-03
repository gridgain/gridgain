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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Abstract interop configuration closure.
 */
public abstract class PlatformAbstractConfigurationClosure
    implements IgniteClosure<IgniteConfiguration, IgniteConfiguration> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether to use platform logger (when custom logger is defined on platform side). */
    protected final boolean useLogger;

    /** Native gateway. */
    protected final PlatformCallbackGateway gate;

    /**
     * Constructor.
     *
     * @param envPtr Environment pointer.
     * @param useLogger Whether use platform logger or not.
     */
    protected PlatformAbstractConfigurationClosure(long envPtr, boolean useLogger) {
        this.gate = new PlatformCallbackGateway(envPtr);
        this.useLogger = useLogger;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration apply(IgniteConfiguration igniteCfg) {
        assert igniteCfg != null;

        IgniteConfiguration igniteCfg0 = new IgniteConfiguration(igniteCfg);

        apply0(igniteCfg0);

        return igniteCfg0;
    }

    /**
     * Internal apply routine.
     *
     * @param igniteCfg Ignite configuration.
     */
    protected abstract void apply0(IgniteConfiguration igniteCfg);
}