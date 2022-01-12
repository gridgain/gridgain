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

package org.apache.ignite.springdata.proxy;

import org.apache.ignite.client.IgniteClient;

/** Implementation of {@link IgniteProxy} that provides access to Ignite cluster through {@link IgniteClient} instance. */
public class IgniteClientProxy implements IgniteProxy {
    /** {@link IgniteClient} instance to which operations are delegated.  */
    private final IgniteClient cli;

    /** */
    public IgniteClientProxy(IgniteClient cli) {
        this.cli = cli;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheProxy<K, V> getOrCreateCache(String name) {
        return new IgniteCacheClientProxy<>(cli.getOrCreateCache(name));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheProxy<K, V> cache(String name) {
        return new IgniteCacheClientProxy<>(cli.cache(name));
    }
}
