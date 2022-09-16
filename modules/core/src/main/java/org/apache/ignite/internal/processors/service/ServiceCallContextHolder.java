/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.services.ServiceCallContext;
import org.jetbrains.annotations.Nullable;

/**
 * Holder of the context of the service call.
 */
public class ServiceCallContextHolder {
    /** Service call context of the current thread. */
    private static final ThreadLocal<ServiceCallContext> locCallCtx = new ThreadLocal<>();

    /**
     * @return Service call context of the current thread.
     */
    @Nullable public static ServiceCallContext current() {
        return locCallCtx.get();
    }

    /**
     * @param callCtx Service call context of the current thread.
     */
    static void current(@Nullable ServiceCallContext callCtx) {
        if (callCtx != null)
            locCallCtx.set(callCtx);
        else
            locCallCtx.remove();
    }
}
