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

package org.apache.ignite.internal.visor.cache.index;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task that triggers indexes force rebuild for cpecified caches or cache groups.
 */
@GridInternal
public class IndexForceRebuildTask extends VisorOneNodeTask<IndexForceRebuildTaskArg, IndexForceRebuildTaskRes> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected IndexForceRebuildJob job(IndexForceRebuildTaskArg arg) {
        return new IndexForceRebuildJob(arg, debug);
    }

    /** */
    private static class IndexForceRebuildJob extends VisorJob<IndexForceRebuildTaskArg, IndexForceRebuildTaskRes> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected IndexForceRebuildJob(@Nullable IndexForceRebuildTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IndexForceRebuildTaskRes run(@Nullable IndexForceRebuildTaskArg arg)
            throws IgniteException
        {
            //Either cacheNames or cacheGrps must be specified
            assert (arg.cacheNames() == null) != (arg.cacheGrps() == null);

            final GridCacheProcessor cacheProcessor = ignite.context().cache();

            // Collect info about indexes being rebuit.
            Set<IndexRebuildStatusInfoContainer> rebuildIdxCaches =
                cacheProcessor.publicCaches()
                    .stream()
                    .filter(c -> !c.indexReadyFuture().isDone())
                    .map(this::fromIgniteCache)
                    .collect(Collectors.toSet());

            Set<String> rebuildIdxCachesNames = rebuildIdxCaches.stream()
                                                                .map(IndexRebuildStatusInfoContainer::cacheName)
                                                                .collect(Collectors.toSet());

            if (arg.cacheNames() != null) {
                // Collect info about not found caches.
                Set<String> notFoundCaches = new HashSet<>(arg.cacheNames());
                notFoundCaches.removeIf(name -> cacheProcessor.cache(name) != null);

                Set<GridCacheContext> cacheContexts = cacheProcessor.publicCaches()
                                                .stream()
                                                .filter(c -> !rebuildIdxCachesNames.contains(c.getName()))
                                                .filter(c -> arg.cacheNames().contains(c.getName()))
                                                .map(IgniteCacheProxy::context)
                                                .collect(Collectors.toSet());

                // Collect info about started index rebuild.
                Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild =
                    cacheContexts.stream()
                        .map(c -> new IndexRebuildStatusInfoContainer(c.config()))
                        .collect(Collectors.toSet());

                cacheProcessor.context().database().forceRebuildIndexes(cacheContexts);

                return new IndexForceRebuildTaskRes(cachesWithStartedRebuild, rebuildIdxCaches, notFoundCaches);
            }
            else if (arg.cacheGrps() != null) {
                // Collect info about not found groups.
                Set<String> notFoundGroups = new HashSet<>(arg.cacheGrps());
                notFoundGroups.removeIf(grpName -> cacheProcessor.findCacheGroup(grpName) != null);

                Set<GridCacheContext> cacheContexts = cacheProcessor.cacheGroups()
                                                .stream()
                                                .filter(grpContext -> arg.cacheGrps().contains(grpContext.name()))
                                                .map(CacheGroupContext::caches)
                                                .flatMap(List::stream)
                                                .filter(c -> !rebuildIdxCachesNames.contains(c.name()))
                                                .collect(Collectors.toSet());

                Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild =
                    cacheContexts.stream()
                        .map(c -> new IndexRebuildStatusInfoContainer(c.config()))
                        .collect(Collectors.toSet());

                cacheProcessor.context().database().forceRebuildIndexes(cacheContexts);

                return new IndexForceRebuildTaskRes(cachesWithStartedRebuild, rebuildIdxCaches, notFoundGroups);
            } else {
                assert false : "Neither cache names nor cache groups specified";

                return null;
            }
        }

        /** */
        private IndexRebuildStatusInfoContainer fromIgniteCache(IgniteCache c) {
            return new IndexRebuildStatusInfoContainer((CacheConfiguration)c.getConfiguration(CacheConfiguration.class));
        }
    }
}
