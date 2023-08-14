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

package org.apache.ignite.internal.processors.io;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;

import static java.util.Objects.nonNull;

/**
 *  Factory for BulkLoadCommandProcessor implementations.
 *  If there is no extensions defined, returns {@link org.apache.ignite.internal.processors.io.BasicBulkLoadCommandProcessor}.
 */
public class BulkLoadCommandProcessorFactory {

    private final IgniteLogger log;

    private BulkLoadCommandProcessor bulkLoadCommandProcessorExt;

    private final BulkLoadCommandProcessor basicBulkLoadCommandProcessor;

    public BulkLoadCommandProcessorFactory(GridKernalContext ctx) {
        this.log = ctx.log(BulkLoadCommandProcessorFactory.class);
        this.bulkLoadCommandProcessorExt = loadFactoryExtension(ctx);
        basicBulkLoadCommandProcessor = new BasicBulkLoadCommandProcessor();
    }

    private BulkLoadCommandProcessor loadFactoryExtension(GridKernalContext ctx) {
        if (nonNull(ctx.plugins())) {
            BulkLoadCommandProcessor[] ext = ctx.plugins().extensions(BulkLoadCommandProcessor.class);

            if (nonNull(ext)) {
                if (ext.length == 1)
                    return ext[0];
                if (ext.length > 1)
                    log.info("More than one BulkLoadFactory extension is defined.");
            }
        }
        return null;
    }

    /**
     * Returns BulkLoadCommandProcessor implementation depending on enabled extensions and serverBulkLoadEnabled
     * param
     */
    public BulkLoadCommandProcessor getBulkLoadCommandProcessor(boolean serverBulkloadEnabled) {
        if (bulkLoadCommandProcessorExt != null && serverBulkloadEnabled) {
            return bulkLoadCommandProcessorExt;
        } else {
            return basicBulkLoadCommandProcessor;
        }
    }
}
