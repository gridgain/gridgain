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

package org.apache.ignite.testframework;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.resource.DependencyResolver;

/**
 * The abstract implementation with registry inside, which provide a dependency on demand by {@link #getDependency(Class)}.
 */
public abstract class AbstractTestDependencyResolver implements DependencyResolver {
    /** Dependency registry. */
    public final ConcurrentMap<Class, Object> registry = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public <T> T resolve(T instance) {
        T resolved = doResolve(instance);

        registry.put(instance.getClass(), resolved);

        return resolved;
    }

    /**
     * Returns demanded dependency if it was registered.
     *
     * @param clazz Required type of dependency.
     *
     * @return Returns {@code null} if dependency wasn't registered.
     */
    public <T> T getDependency(Class<T> clazz) {
        return (T) registry.get(clazz);
    }

    /**
     * Custom resolver logic.
     */
    protected abstract  <T> T doResolve(T instance);
}
