/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.mxbean.ServiceMXBean;

/**
 * ServiceMXBean implementation.
 */
public class ServiceMXBeanImpl implements ServiceMXBean {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public ServiceMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(ServiceMXBeanImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void cancel(String name) {
        A.notNull(name, "name");

        if (log.isInfoEnabled())
            log.info("Canceling service[name=" + name + ']');

        try {
            ctx.grid().services().cancel(name);
        }
        catch (IgniteException e) {
            throw new RuntimeException(e);
        }
    }
}
