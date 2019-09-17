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
    /** Action name. */
    private final String actName;
    /** Method. */
    private final Method mtd;

    /** Controller class. */
    private final Class<?> controllerCls;

    /** Is need auth. */
    private final boolean isNeedAuth;

    /**
     * @param actName Action name.
     * @param mtd Method.
     * @param controllerCls Controller class.
     */
    public ActionMethod(String actName, Method mtd, Class<?> controllerCls, boolean isNeedAuth) {
        this.actName = actName;
        this.mtd = mtd;
        this.controllerCls = controllerCls;
        this.isNeedAuth = isNeedAuth;
    }

    /**
     * @return Controller class.
     */
    public Class<?> getControllerClass() {
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
        return actName;
    }

    /**
     * @return {@code True} if action requiere authentication.
     */
    public boolean isNeedAuth() {
        return isNeedAuth;
    }
}
