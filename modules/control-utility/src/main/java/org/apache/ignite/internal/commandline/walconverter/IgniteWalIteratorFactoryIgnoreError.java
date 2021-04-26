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

package org.apache.ignite.internal.commandline.walconverter;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Factory for creating iterator over WAL files
 */
public class IgniteWalIteratorFactoryIgnoreError extends IgniteWalIteratorFactory {
    /**
     * @param iteratorParametersBuilder Iterator parameters builder.
     * @return closable WAL records iterator, should be closed when non needed
     */
    @Override
    public WALIterator iterator(
        @NotNull IteratorParametersBuilder iteratorParametersBuilder
    ) throws IgniteCheckedException, IllegalArgumentException {
        if (true){
            return super.iterator(iteratorParametersBuilder);
        }
        iteratorParametersBuilder.validate();

        return new StandaloneWalRecordsIteratorIgnoreError(
            iteratorParametersBuilder.getLog() == null ? log : iteratorParametersBuilder.getLog(),
            iteratorParametersBuilder.getSharedCtx() == null ? prepareSharedCtx(iteratorParametersBuilder) :
                iteratorParametersBuilder.getSharedCtx(),
            iteratorParametersBuilder.getIoFactory(),
            resolveWalFiles(iteratorParametersBuilder),
            iteratorParametersBuilder.getFilter(),
            iteratorParametersBuilder.getLowBound(),
            iteratorParametersBuilder.getHighBound(),
            iteratorParametersBuilder.isKeepBinary(),
            iteratorParametersBuilder.getBufferSize(),
            iteratorParametersBuilder.isStrictBoundsCheck()
        );
    }
}
