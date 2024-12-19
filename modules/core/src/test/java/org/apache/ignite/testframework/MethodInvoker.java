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

    /** Method reference */
    protected MethodRef methodRef;

    /** Marker of accessibility flag inversion. */
    protected boolean accessFlipped = false;

    /** Constructor */
    protected MethodInvoker(MethodRef methodRef) {
        this.methodRef = methodRef;
    }

    /** Get method instance */
    protected Method method() {
        return methodRef.get();
    }

    /**  */
    @SuppressWarnings("unchecked")
    protected <T> T invoke0(@Nullable Object obj, Object... args) {
        try {
            return (T)method().invoke(obj, args);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e.getMessage() + "\nMake sure that you've called init()", e);
        }
        catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /** Lookup the method instance and make it accessible if needed */
    @SuppressWarnings("unchecked")
    public SELF init() {
        if (!method().isAccessible()) {
            accessFlipped = true;
            method().setAccessible(true);
        }

        return (SELF)this;
    }

    /** Reset accessibility flag if it was changed */
    @SuppressWarnings("unchecked")
    public SELF release() {
        if (accessFlipped)
            method().setAccessible(false);

        return (SELF)this;
    }

    /** Get an invoker for specified method */
    public static Regular of(Class<?> cls, String name, Class<?>... params) {
        return new Regular(new MethodRef(cls, name, params));
    }

    /**
     * Get an invoker for specified method bound to provided object instance
     * (all invocations will use provided object instance)
     */
    public static Bound bound(Object instance, String name, Class<?>... params) {
        return new Bound(instance, new MethodRef(instance.getClass(), name, params), false);
    }

    /** Get an invoker for specified static method */
    public static Static ofStatic(Class<?> cls, String name, Class<?>... params) {
        return new Static(new MethodRef(cls, name, params));
    }

    /** Regular method invoker with variable instances */
    public static class Regular extends MethodInvoker<Regular> {
        private Regular(MethodRef lookup) {
            super(lookup);
        }

        /**
         * @param args Method arguments
         */
        public <T> T invoke(Object instance, Object... args) {
            return invoke0(instance, args);
        }

        public Bound bindWith(@Nullable Object instance) {
            return new Bound(instance, methodRef, accessFlipped);
        }
    }

    /** Regular method invoker with constant instance */
    public static class Bound extends MethodInvoker<Bound> {
        private final Object instance;

        private Bound(@Nullable Object instance, MethodRef methodRef, boolean isAccessible) {
            super(methodRef);
            this.instance = instance;
            this.accessFlipped = isAccessible;
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
        private Static(MethodRef methodRef) {
            super(methodRef);
        }

        /**
         * @param argsOnly Method arguments, no need to provide null as a marker of static invocation
         */
        public <T> T invoke(Object... argsOnly) {
            return invoke0(null, argsOnly);
        }
    }
}
