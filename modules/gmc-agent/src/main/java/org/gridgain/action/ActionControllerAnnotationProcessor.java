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

package org.gridgain.action;

import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Proccessor for {@link ActionController} annotation.
 */
public class ActionControllerAnnotationProcessor {
    /** Methods. */
    private static Map<String, ActionMethod> methods = new HashMap<>();

    /**
     * Find the action methods by default package.
     */
    public static Map<String, ActionMethod> findActionMethods() {
        return findActionMethods("org.gridgain.action.controller");
    }

    /**
     * Find the action methods by specific package.
     *
     * @param basePkg Base package.
     */
    public static Map<String, ActionMethod> findActionMethods(String basePkg) {
        if (!methods.isEmpty())
            return Collections.unmodifiableMap(methods);

        Reflections reflections = new Reflections(basePkg);
        for (Class<?> controllerCls : reflections.getTypesAnnotatedWith(ActionController.class)) {
            for (Method method : controllerCls.getDeclaredMethods()) {
                ActionMethod actMtd = new ActionMethod(method, controllerCls);
                methods.put(actMtd.getActionName(), actMtd);
            }
        }

        return Collections.unmodifiableMap(methods);
    }
}
