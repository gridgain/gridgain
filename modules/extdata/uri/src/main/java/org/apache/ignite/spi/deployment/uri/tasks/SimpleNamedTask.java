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

package org.apache.ignite.spi.deployment.uri.tasks;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toMap;

/**
 * A simpliest possible named task (it does nothing).
 */
@ComputeTaskName("SimpleNamedTask")
public class SimpleNamedTask extends ComputeTaskAdapter<String, String> {
    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, @Nullable String arg
    ) throws IgniteException {
        return subgrid.stream().collect(toMap(node -> new TestJob(), Function.identity()));
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
        return "ok";
    }

    /**
     * Does nothing.
     */
    private static class TestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            return null;
        }
    }
}
