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

package org.apache.ignite.internal.visor.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.query.VisorQueryUtils;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Task that get value in specified cache for specified key value.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheGetValueTask extends VisorOneNodeTask<VisorCacheGetValueTaskArg, VisorCacheModifyTaskResult> {
    /** */
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheModifyJob job(VisorCacheGetValueTaskArg arg) {
        return new VisorCacheModifyJob(arg, debug);
    }

    /**
     * Job that get value in specified cache for specified key value.
     */
    private static class VisorCacheModifyJob extends VisorJob<VisorCacheGetValueTaskArg, VisorCacheModifyTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheModifyJob(VisorCacheGetValueTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * Convert string value to value of specified type.
         *
         * @param type Expected type.
         * @param value String presentation of value.
         * @return Value of specified type.
         */
        private Object parseArgumentValue(VisorObjectType type, String value) {
            switch (type) {
                case CHARACTER:
                    return value.charAt(0);

                case INT:
                    return Integer.valueOf(value);

                case LONG:
                    return Long.valueOf(value);

                case SHORT:
                    return Short.valueOf(value);

                case BYTE:
                    return Byte.valueOf(value);

                case FLOAT:
                    return Float.valueOf(value);

                case DOUBLE:
                    return Double.valueOf(value);

                case BOOLEAN:
                    return Boolean.valueOf(value);

                case UUID:
                    return UUID.fromString(value);

                case TIMESTAMP:
                    return new Timestamp(Long.parseLong(value));

                case DATE:
                    return new Date(Long.parseLong(value));

                case BINARY:
                    try {
                        JsonNode obj = MAPPER.readTree(value);

                        return constructBinaryValue(obj);
                    }
                    catch (IOException e) {
                        throw new IgniteException("Failed to read key object", e);
                    }

                default:
                    return value;
            }
        }

        /**
         * Construct {@link BinaryObject} value specified in JSON object.
         *
         * @param value JSON specification of {@link BinaryObject} value.
         * @return {@link BinaryObject} for specified value.
         */
        private BinaryObject constructBinaryValue(JsonNode value) {
            String clsName = value.get("className").textValue();

            BinaryObjectBuilder b = ignite.binary().builder(clsName);

            for (Iterator<JsonNode> it = value.get("fields").elements(); it.hasNext();) {
                JsonNode fld = it.next();

                String fldName = fld.get("name").textValue();
                VisorObjectType fldType = VisorObjectType.valueOf(fld.get("type").textValue());
                JsonNode fldValue = fld.get("value");

                if (fldType == VisorObjectType.BINARY)
                    b.setField(fldName, constructBinaryValue(fldValue));
                else
                    b.setField(fldName, parseArgumentValue(fldType, fldValue.asText()));
            }

            return b.build();
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheModifyTaskResult run(final VisorCacheGetValueTaskArg arg) {
            assert arg != null;

            String cacheName = arg.getCacheName();
            assert cacheName != null;

            @Nullable IgniteCache<Object, Object> cache = ignite.cache(cacheName);

            ignite.context().cache().internalCache(cacheName);

            if (cache == null)
                throw new IllegalArgumentException("Failed to find cache with specified name [cacheName=" + arg.getCacheName() + "]");

            String keyStr = arg.getKey();
            assert keyStr != null;
            assert !keyStr.isEmpty();

            Object key = null;

            try {
                VisorObjectType type = VisorObjectType.valueOf(arg.getType());

                key = parseArgumentValue(type, keyStr);
            }
            catch (IllegalArgumentException iae) {
                throw new IgniteException("Specified key type is not supported", iae);
            }

            assert key != null;

            ClusterNode node = ignite.affinity(cacheName).mapKeyToNode(key);

            UUID nid = node != null ? node.id() : null;

            Object val = cache.withKeepBinary().get(key);

            return new VisorCacheModifyTaskResult(nid, VisorTaskUtils.compactClass(val), val);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheModifyJob.class, this);
        }
    }
}
