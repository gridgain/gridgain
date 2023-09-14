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

package org.apache.ignite.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Task to test nullable primitives interop behavior.
 */
public class PlatformNullablePrimitivesTask extends ComputeTaskAdapter<String, Long> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String arg) {
        return Collections.singletonMap(new NullablePrimitivesJob(arg), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Long reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class NullablePrimitivesJob extends ComputeJobAdapter {
        private final String arg;

        @IgniteInstanceResource
        private Ignite ignite;

        public NullablePrimitivesJob(String arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            String[] args = arg.split("\\|");
            String cmd = args[0];
            String cacheName = args[1];
            boolean nulls = Boolean.parseBoolean(args[2]);

            IgniteCache<Integer, JavaNullableValueTypes> cache = ignite.cache(cacheName);

            switch (cmd.toLowerCase()) {
                case "put":
                    JavaNullableValueTypes primitives = new JavaNullableValueTypes();

                    if (!nulls) {
                        primitives.Byte = 1;
                        primitives.Bytes = new Byte[] {1, 2};
                        primitives.Bool = true;
                        primitives.Bools = new Boolean[] {true, false};
                        primitives.Char = 'a';
                        primitives.Chars = new Character[] {'a', 'b'};
                        primitives.Short = 1;
                        primitives.Shorts = new Short[] {1, 2};
                        primitives.Int = 1;
                        primitives.Ints = new Integer[] {1, 2};
                        primitives.Long = 1L;
                        primitives.Longs = new Long[] {1L, 2L};
                        primitives.Float = 1.0f;
                        primitives.Floats = new Float[] {1.0f, 2.0f};
                        primitives.Double = 1.0;
                        primitives.Doubles = new Double[] {1.0, 2.0};
                        primitives.Guid = UUID.randomUUID();
                        primitives.Guids = new UUID[] {UUID.randomUUID(), UUID.randomUUID()};
                    }

                    cache.put(1, primitives);
                    break;

                case "get":
                    cache.get(1);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown command: " + cmd);
            }

            return null;
        }
    }

    public static class JavaNullableValueTypes
    {
        public Byte Byte;
        public Byte[] Bytes;
        public Boolean Bool;
        public Boolean[] Bools;
        public Character Char;
        public Character[] Chars;
        public Short Short;
        public Short[] Shorts;
        public Integer Int;
        public Integer[] Ints;
        public Long Long;
        public Long[] Longs;
        public Float Float;
        public Float[] Floats;
        public Double Double;
        public Double[] Doubles;
        public UUID Guid;
        public UUID[] Guids;
    }
}