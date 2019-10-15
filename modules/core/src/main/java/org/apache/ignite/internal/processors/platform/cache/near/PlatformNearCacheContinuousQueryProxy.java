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

package org.apache.ignite.internal.processors.platform.cache.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.cache.query.PlatformContinuousQuery;

/**
 * Proxy that implements PlatformTarget.
 */
public class PlatformNearCacheContinuousQueryProxy extends PlatformAbstractTarget  {
    private final PlatformNearCacheContinuousQuery qry;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    public PlatformNearCacheContinuousQueryProxy(PlatformContext platformCtx, PlatformNearCacheContinuousQuery qry) {
        super(platformCtx);

        assert qry != null;

        this.qry = qry;
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) {
        qry.close();

        return 0;
    }
}
