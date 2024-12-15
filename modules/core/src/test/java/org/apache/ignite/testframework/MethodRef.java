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

/**
 * Lazy wrapper around method instance accessible via Java reflection.
 * See also {@link MethodInvoker}
 */
public class MethodRef {

    /** Method owner */
    private final Class<?> cls;

    /** Method name */
    private final String name;

    /** Method params */
    private final Class<?>[] params;

    /** Method reference */
    private volatile Method method;

    /** Constructor */
    public MethodRef(Class<?> cls, String name, Class<?>... params) {
        this.cls = cls;
        this.name = name;
        this.params = params;
    }

    /** Search for method and preserve result for further usage */
    public Method get() {
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

    /**
     * Invoke method. Does not check or adjust method accessibility!
     * If you need automatic adjustments via {@link Method#setAccessible(boolean)} use {@link MethodInvoker} instead
     */
    @SuppressWarnings("unchecked")
    public <T> T invoke(@Nullable Object obj, Object... args) {
        try {
            return (T)get().invoke(obj, args);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
