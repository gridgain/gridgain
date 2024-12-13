/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.jetbrains.annotations.Nullable;

/** Wrapper around class method that encapsulates invocation boilerplate */
public abstract class MethodInvoker<SELF extends MethodInvoker<SELF>> {

    protected Lookup lookup;
    protected boolean accessible;

    protected MethodInvoker(Lookup lookup) {
        this.lookup = lookup;
    }

    protected Method method() {
        return lookup.get();
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke0(@Nullable Object obj, Object... args) {
        try {
            return (T)method().invoke(obj, args);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /** Lookup the method instance and make it accessible if needed */
    @SuppressWarnings("unchecked")
    public SELF init() {
        accessible = method().isAccessible();
        if (!accessible) {
            method().setAccessible(true);
        }

        return (SELF)this;
    }

    /** Reset accessibility flag if it was changed */
    @SuppressWarnings("unchecked")
    public SELF release() {
        method().setAccessible(accessible);
        return (SELF)this;
    }

    /** Get an invoker for specified method */
    public static Instance of(Class<?> cls, String name, Class<?>... params) {
        return new Instance(new Lookup(cls, name, params));
    }

    /**
     * Get an invoker for specified method bound to provided object instance
     * (all invocations will use provided object instance)
     */
    public static Bound bound(Object instance, String name, Class<?>... params) {
        return new Bound(instance, new Lookup(instance.getClass(), name, params), false);
    }

    /** Get an invoker for specified static method */
    public static Static ofStatic(Class<?> cls, String name, Class<?>... params) {
        return new Static(new Lookup(cls, name, params));
    }

    /** Regular method invoker with variable instances */
    public static class Instance extends MethodInvoker<Instance> {
        private Instance(Lookup lookup) {
            super(lookup);
        }

        /**
         * @param args Method arguments
         */
        public <T> T invoke(Object instance, Object... args) {
            return invoke0(instance, args);
        }

        public Bound bindWith(@Nullable Object instance) {
            return new Bound(instance, lookup, accessible);
        }
    }

    /** Regular method invoker with constant instance */
    public static class Bound extends MethodInvoker<Bound> {
        private final Object instance;

        private Bound(@Nullable Object instance, Lookup lookup, boolean isAccessible) {
            super(lookup);
            this.instance = instance;
            this.accessible = isAccessible;
        }

        /**
         * @param argsOnly Method arguments, no need to provide null as a marker of static invocation
         */
        public <T> T invoke(Object... argsOnly) {
            return invoke0(instance, argsOnly);
        }
    }

    /** Static method invoker */
    public static class Static extends MethodInvoker<Static> {
        private Static(Lookup lookup) {
            super(lookup);
        }

        /**
         * @param argsOnly Method arguments, no need to provide null as a marker of static invocation
         */
        public <T> T invoke(Object... argsOnly) {
            return invoke0(null, argsOnly);
        }
    }

    private static class Lookup {
        private final Class<?> cls;
        private final String name;
        private final Class<?>[] params;
        private volatile Method method;

        public Lookup(Class<?> cls, String name, Class<?>... params) {
            this.cls = cls;
            this.name = name;
            this.params = params;
        }

        private Method get() {
            if (method == null) {
                synchronized (this) {
                    if (method == null) {
                        try {
                            method = GridTestUtils.methodLookup(cls, name, params).orElseThrow(() -> new RuntimeException(
                                "Method <" + method + "> not found in " + cls.getName() + " or it's ancestors"
                            ));
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }

            return method;
        }
    }

}
