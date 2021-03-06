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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;

/**
 * Test plugin provider for test strategies.
 */
public class WarmUpTestPluginProvider extends AbstractTestPluginProvider {
    /** Collection of strategies. */
    public final List<WarmUpStrategy<?>> strats = new ArrayList<>(Arrays.asList(
        new SimpleObservableWarmUpStrategy(),
        new BlockedWarmUpStrategy()
    ));

    /** {@inheritDoc} */
    @Override public String name() {
        return getClass().getSimpleName();
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        super.initExtensions(ctx, registry);

        IgniteEx gridx = (IgniteEx)ctx.grid();

        strats.add(new LoadAllWarmUpStrategyEx(gridx.log(), () -> gridx.context().cache().cacheGroups()));

        registry.registerExtension(WarmUpStrategySupplier.class, new WarmUpStrategySupplier() {
            /** {@inheritDoc} */
            @Override public Collection<WarmUpStrategy<?>> strategies() {
                return strats;
            }
        });
    }
}
