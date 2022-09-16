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

package org.apache.ignite.internal.processors.resource;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteServicesEx;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.service.ServiceCallContextHolder;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.jetbrains.annotations.Nullable;

/**
 * Grid service injector.
 */
public class GridResourceServiceInjector extends GridResourceBasicInjector<Collection<Service>> {
    /** */
    private IgniteEx ignite;

    /**
     * @param ignite Grid.
     */
    public GridResourceServiceInjector(IgniteEx ignite) {
        super(null);

        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        Object svc = getService((ServiceResource)field.getAnnotation());

        if (svc != null)
            GridResourceUtils.inject(field.getField(), target, svc);
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws IgniteCheckedException {
        Object svc = getService((ServiceResource)mtd.getAnnotation());

        if (svc != null)
            GridResourceUtils.inject(mtd.getMethod(), target, svc);
    }

    /**
     * @param ann Service resource annotation.
     * @return Proxy for the service if a proxy interface was specified, otherwise the service itself or {@code null}
     *         if the service is not deployed locally.
     */
    private @Nullable <T> T getService(ServiceResource ann) {
        if (ann.proxyInterface() == Void.class)
            return ignite.services().service(ann.serviceName());

        return ((IgniteServicesEx)ignite.services()).serviceProxy(
            ann.serviceName(),
            (Class<? super T>)ann.proxyInterface(),
            ann.proxySticky(),
            ann.forwardCallerContext() ? ServiceCallContextHolder::current : null,
            0
        );
    }
}
