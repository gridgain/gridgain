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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.gridgain.dto.action.Request;

import static org.gridgain.action.ActionControllerAnnotationProcessor.getActions;

/**
 * Action dispatcher.
 */
public class ActionDispatcher implements AutoCloseable {
    /** Context. */
    private final GridKernalContext ctx;

    /** Controllers. */
    private final Map<String, Object> controllers = new HashMap<>();

    /** Thread pool. */
    private final ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

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
    public CompletableFuture<CompletableFuture> dispatch(Request req) {
        String act = req.getAction();

        ActionMethod mtd = getActions().get(act);

        if (mtd == null)
            throw new IgniteException("Failed to find action method");

        return CompletableFuture.supplyAsync(() -> invoke(mtd, req.getArgument()), pool);
    }

    /**
     * Invoke action method.
     *
     * @param mtd Method.
     * @param arg Argument.
     */
    private CompletableFuture invoke(ActionMethod mtd, Object arg) {
        try {
            if (!controllers.containsKey(mtd.getActionName()))
                controllers.put(mtd.getActionName(), mtd.getControllerClass().getConstructor(GridKernalContext.class).newInstance(ctx));

            return (CompletableFuture) mtd.getMethod().invoke(controllers.get(mtd.getActionName()), arg);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        pool.shutdown();
    }
}
