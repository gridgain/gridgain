/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.plugin.Extension;

/**
 * BulkLoadCommandProcessor allows to process bulk load (COPY) commands.
 * This interface defines an extension that can be provided by a third party.
 */
public interface BulkLoadCommandProcessor extends Extension {

    /**
     * Processes bulk load (COPY) command.
     *
     * @param ctx GridKernalContext.
     * @param cmd SqlBulkLoadCommand.
     * @param qryId Query ID.
     * @return FieldsQueryCursor
     * @throws IgniteCheckedException
     */
    FieldsQueryCursor<List<?>> processBulkLoadCommand(
        GridKernalContext ctx,
        SqlBulkLoadCommand cmd,
        Long qryId) throws IgniteCheckedException;
}
