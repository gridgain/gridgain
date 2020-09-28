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

package org.apache.ignite.internal.managers.systemview;

import java.util.function.Predicate;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.systemview.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.systemview.SystemViewExporterSpi;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.jetbrains.annotations.Nullable;

/**
 * Basic class for {@link SystemViewExporterSpi} implementations.
 *
 * @see JmxSystemViewExporterSpi
 * @see IgniteConfiguration#setSystemViewExporterSpi(SystemViewExporterSpi...)
 */
public abstract class AbstractSystemViewExporterSpi extends IgniteSpiAdapter implements SystemViewExporterSpi {
    /** System view registry. */
    protected ReadOnlySystemViewRegistry sysViewReg;

    /** System view filter. */
    @Nullable protected Predicate<SystemView<?>> filter;

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setSystemViewRegistry(ReadOnlySystemViewRegistry mlreg) {
        this.sysViewReg = mlreg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<SystemView<?>> filter) {
        this.filter = filter;
    }
}
