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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.gridgain.dto.action.Request;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.gridgain.action.ActionControllerAnnotationProcessor.findActionMethods;

/**
 * Action dispatcher.
 */
public class ActionDispatcher {
    /** Context. */
    private final GridKernalContext ctx;

    /** Controllers. */
    private final Map<String, Object> controllers = new HashMap<>();

    /**
     * @param ctx Context.
     */
    public ActionDispatcher(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Find the controller with appropriate method and invoke it.
     *
     * @param req Request.
     * @return Completable future with action result.
     */
    public CompletableFuture dispatch(Request req) throws Exception {
        String actName = req.getActionName();

        ActionMethod mtd = findActionMethods().get(actName);

        if (mtd == null)
            throw new IgniteException("Action not found");

        return invoke(mtd, req.getArgument());
    }

    /**
     * Invoke action method.
     *
     * @param mtd Method.
     * @param arg Argument.
     */
    private CompletableFuture invoke(ActionMethod mtd, Object arg) throws Exception {
        if (!controllers.containsKey(mtd.getActionName()))
            controllers.put(mtd.getActionName(), mtd.getControllerCls().getConstructor(GridKernalContext.class).newInstance(ctx));

        return (CompletableFuture) mtd.getMethod().invoke(controllers.get(mtd.getActionName()), arg);
    }
}
