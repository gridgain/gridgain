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

import java.lang.reflect.Method;

/**
 * ACtion method structure.
 */
public class ActionMethod {
    /** Method. */
    private final Method mtd;

    /** Controller class. */
    private final Class<?> controllerCls;

    /**
     * @param mtd Method.
     * @param controllerCls Controller class.
     */
    public ActionMethod(Method mtd, Class<?> controllerCls) {
        this.mtd = mtd;
        this.controllerCls = controllerCls;
    }

    /**
     * @return Controller class.
     */
    public Class<?> getControllerCls() {
        return controllerCls;
    }

    /**
     * @return Action method.
     */
    public Method getMethod() {
        return mtd;
    }

    /**
     * @return Action name.
     */
    public String getActionName() {
        return controllerCls.getSimpleName() + "." + mtd.getName();
    }
}
