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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.StripedExecutor.Stripe;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * {@link StripedExecutor} task representation for a {@link SystemView}.
 */
public class StripedExecutorTaskView {
    /** Stripe. */
    private final Stripe stripe;

    /** Task */
    private final Runnable task;

    /**
     * @param stripe Stripe.
     * @param task Task.
     */
    public StripedExecutorTaskView(Stripe stripe, Runnable task) {
        this.stripe = stripe;
        this.task = task;
    }

    /** @return Stripe index for task. */
    @Order
    public int stripeIndex() {
        return stripe.index();
    }

    /** @return Task class name. */
    @Order(3)
    public String taskName() {
        return task.getClass().getName();
    }

    /** @return Task {@code toString} representation. */
    @Order(1)
    public String description() {
        return toStringSafe(task);
    }

    /** @return Name of the {@link Thread} executing the {@link #task}. */
    @Order(2)
    public String threadName() {
        return stripe.name();
    }
}
