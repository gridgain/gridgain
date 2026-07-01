/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

import java.util.Arrays;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.services.ServiceCallInterceptor;
import org.apache.ignite.services.ServiceConfiguration;

/**
 * Lazy service configuration carrying marshalled service call interceptors.
 *
 * <p>Kept as a subclass of {@link LazyServiceConfiguration} instead of adding fields to it directly so that the
 * wire format of the base class stays unchanged for peers that don't support the
 * {@code IgniteFeatures#SERVICE_CALL_INTERCEPTORS} feature. Only construct this class once
 * {@code IgniteFeatures.allNodesSupport(ctx, IgniteFeatures.SERVICE_CALL_INTERCEPTORS)} is {@code true} for the
 * whole cluster (see GG-49638).
 */
public class LazyServiceConfigurationV2 extends LazyServiceConfiguration {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service interceptors. */
    @GridToStringExclude
    private transient ServiceCallInterceptor[] interceptors;

    /** */
    private byte[] interceptorsBytes;

    /**
     * Default constructor.
     */
    public LazyServiceConfigurationV2() {
        // No-op.
    }

    /**
     * @param cfg Configuration.
     * @param srvcBytes Marshalled service.
     * @param interceptorsBytes Marshalled interceptors.
     */
    public LazyServiceConfigurationV2(ServiceConfiguration cfg, byte[] srvcBytes, byte[] interceptorsBytes) {
        super(cfg, srvcBytes);

        interceptors = cfg.getInterceptors();
        this.interceptorsBytes = interceptorsBytes;
    }

    /** {@inheritDoc} */
    @Override public ServiceCallInterceptor[] getInterceptors() {
        return interceptors;
    }

    /**
     * @return Interceptors bytes.
     */
    public byte[] interceptorBytes() {
        return interceptorsBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean equalsIgnoreNodeFilter(Object o) {
        if (!super.equalsIgnoreNodeFilter(o))
            return false;

        LazyServiceConfigurationV2 that = (LazyServiceConfigurationV2)o;

        return Arrays.equals(interceptorsBytes, that.interceptorsBytes);
    }
}
