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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.IntSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**  */
@GridInternal
@GridVisorManagementTask
public class VisorDrResetTreeTask extends VisorMultiNodeTask<
    VisorDrResetTreeTask.VisorDrResetTreeTaskArgs,
    VisorDrResetTreeTask.VisorDrResetTreeTaskResult,
    VisorDrResetTreeTask.VisorDrResetTreeJobResult> {
    /**  */
    private static final long serialVersionUID = 0L;

    @Override protected VisorJob<VisorDrResetTreeTaskArgs, VisorDrResetTreeJobResult> job(VisorDrResetTreeTaskArgs arg) {
        return new ResetTreeJob(arg, debug);
    }

    @Override protected @Nullable VisorDrResetTreeTaskResult reduce0(List<ComputeJobResult> jobResults) throws IgniteException {
        StringBuilder msg = new StringBuilder();
        Map<String, IntSet> taskResult = new HashMap<>();

        for (ComputeJobResult res : jobResults) {
            msg.append(res.getNode().consistentId()).append(":\n");

            if (res.getData() == null)
                msg.append("    err=").append(res.getException()).append('\n');
            else {
                VisorDrResetTreeJobResult data = res.getData();

                if (F.isEmpty(data.errors))
                    msg.append("    No errors.\n");
                else
                    data.errors().forEach(err -> msg.append("    warn=").append(res.getException()).append('\n'));

                data.result().entrySet().forEach(e -> taskResult
                    .computeIfAbsent(e.getKey(), key -> new BitSetIntSet())
                    .addAll(e.getValue())
                );

                if (debug) {
                    ignite.log().debug(data.result().entrySet().stream()
                        .map(e -> "Reset " + e.getKey() + "=" + S.compact(e.getValue()))
                        .collect(Collectors.joining(";\n        ","", "\n")));
                }
            }
        }

        msg.append("Task result:\n");
        taskResult.forEach((cache, parts) -> msg.append("    ").append(cache).append("=").append(S.compact(parts)).append(System.lineSeparator()));

        return new VisorDrResetTreeTaskResult(msg.toString());
    }

    /**  */
    private static class ResetTreeJob extends VisorJob<VisorDrResetTreeTask.VisorDrResetTreeTaskArgs, VisorDrResetTreeTask.VisorDrResetTreeJobResult> {
        /**  */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected ResetTreeJob(@Nullable VisorDrResetTreeTask.VisorDrResetTreeTaskArgs arg, boolean debug) {
            super(arg, debug);
        }

        @Override protected VisorDrResetTreeJobResult run(VisorDrResetTreeTask.VisorDrResetTreeTaskArgs arg)
            throws IgniteException {
            IntPredicate partitionFilter = arg.partitions() == null ? p -> true : arg.partitions()::contains;
            Predicate<String> cacheFilter = arg.caches == null ? c -> !CU.isSystemCache(c) : arg.caches()::contains;

            Map<String, IntSet> result = new HashMap<>();
            List<String> errors = new ArrayList<>();

            Collection<String> caches = ignite.context().cache().cacheNames().stream()
                .filter(cacheFilter)
                .collect(Collectors.toSet());

            for (String cache : caches) {
                try {
                    GridCacheContext<Object, Object> ctx = ignite.context().cache().cache(cache).context();

                    if (!ctx.affinityNode()) {
                        if (debug)
                            ignite.log().debug("Skip resetting DR state for cache: " + cache);

                        result.put(cache, new BitSetIntSet());

                        continue;
                    }

                    IntSet partitionsProcessed = new BitSetIntSet();
                    ctx.topology().readLock();
                    try {
                        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

                        for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
                            if (partitionFilter.test(part.id()) && part.primary(topVer)) {
                                long updateCounter = part.dataStore().updateCounter();

                                ctx.dr().lwm(part.id(), 0, updateCounter);
                                partitionsProcessed.add(part.id());
                            }
                        }

                        result.put(cache, partitionsProcessed);
                    }
                    finally {
                        ctx.topology().readUnlock();
                    }

                    if (debug)
                        ignite.log().debug("Reset DR state: cache=" + cache + ", partitions=" + Arrays.toString(partitionsProcessed.toIntArray()));
                } catch (Exception ex) {
                    errors.add("Failed to reset DR state: cache=" + cache + ", error=" + ex.getMessage());

                    if (debug)
                        ignite.log().warning("Failed to reset DR state for cache:" + cache, ex);
                }
            }

            return new VisorDrResetTreeJobResult(result, errors);
        }
    }

    /**
     *
     */
    public static class VisorDrResetTreeTaskArgs extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Caches. */
        private Set<String> caches;

        /** Partitions. */
        private Set<Integer> partitions;

        /** */
        public VisorDrResetTreeTaskArgs() {
        }

        /** */
        public VisorDrResetTreeTaskArgs(
            Set<String> caches,
            Set<Integer> partitions
        ) {
            this.caches = caches;
            this.partitions = partitions;
        }

        public Set<String> caches() {
            return caches;
        }

        public Set<Integer> partitions() {
            return partitions;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeCollection(out, caches);
            U.writeIntCollection(out, partitions);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in)
            throws IOException, ClassNotFoundException {
            caches = U.readSet(in);
            partitions = U.readIntSet(in);
        }
    }

    /**
     *
     */
    public static class VisorDrResetTreeJobResult extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Visor job results. */
        private Map<String, IntSet> jobResult;

        /** Job error messages. */
        private List<String> errors;

        /**
         * Default constructor.
         */
        public VisorDrResetTreeJobResult() {
        }

        /**
         * @param jobResult Job result.
         */
        VisorDrResetTreeJobResult(Map<String, IntSet> jobResult, List<String> errors) {
            this.jobResult = jobResult;
            this.errors = errors;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeMap(out, jobResult);
            U.writeCollection(out, errors);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in)
            throws IOException, ClassNotFoundException {
            jobResult = U.readHashMap(in);
            errors = U.readList(in);
        }

        public Map<String, IntSet> result() {
            return jobResult;
        }

        public List<String> errors() {
            return errors;
        }
    }

    /**
     *
     */
    public static class VisorDrResetTreeTaskResult extends IgniteDataTransferObject {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Visor task messages. */
        private List<String> messages;

        /**
         * Default constructor.
         */
        public VisorDrResetTreeTaskResult() {
        }

        /**
         * @param message Task result message.
         */
        VisorDrResetTreeTaskResult(String message) {
            this.messages = Collections.singletonList(message);
        }

        /**
         * @param failures Task failures.
         */
        VisorDrResetTreeTaskResult(List<String> failures) {
            assert !failures.isEmpty();

            this.messages = failures;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeCollection(out, messages);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in)
            throws IOException, ClassNotFoundException {
            messages = U.readList(in);
        }

        public List<String> messages() {
            return messages;
        }
    }
}
