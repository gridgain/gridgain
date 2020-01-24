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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Task that get value in specified cache for specified key value.
 */
@GridInternal
@GridVisorManagementTask
public class VisorCacheGetValueTask extends VisorOneNodeTask<VisorCacheGetValueTaskArg, VisorCacheModifyTaskResult> {
    /** */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheGetValueJob job(VisorCacheGetValueTaskArg arg) {
        return new VisorCacheGetValueJob(arg, debug);
    }

    /**
     * Job that get value in specified cache for specified key value.
     */
    private static class VisorCacheGetValueJob extends VisorJob<VisorCacheGetValueTaskArg, VisorCacheModifyTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheGetValueJob(VisorCacheGetValueTaskArg arg, boolean debug) {
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
                case STRING:
                    return value;

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
                        VisorBinaryClassDescriptor obj = new VisorBinaryClassDescriptor(MAPPER.readTree(value));

                        return constructBinaryValue(obj);
                    }
                    catch (IOException e) {
                        throw new IgniteException("Failed to read key object", e);
                    }

                default:
                    throw new IllegalArgumentException("Specified type is not supported: " + type);
            }
        }

        /**
         * Construct {@link BinaryObject} value specified in JSON object.
         *
         * @param value JSON specification of {@link BinaryObject} value.
         * @return {@link BinaryObject} for specified value.
         */
        private BinaryObject constructBinaryValue(VisorBinaryClassDescriptor value) {
            BinaryObjectBuilder b = ignite.binary().builder(value.className);

            for (VisorBinaryFieldDescription fld: value.fields) {
                if (fld.getType() == VisorObjectType.BINARY)
                    b.setField(fld.getFldName(), constructBinaryValue((VisorBinaryClassDescriptor)fld.value));
                else
                    b.setField(fld.getFldName(), parseArgumentValue(fld.getType(), fld.value.toString()));
            }

            return b.build();
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheModifyTaskResult run(final VisorCacheGetValueTaskArg arg) {
            assert arg != null;

            String cacheName = arg.getCacheName();
            assert cacheName != null;

            @Nullable IgniteCache<Object, Object> cache = ignite.cache(cacheName);

            if (cache == null)
                throw new IllegalArgumentException("Failed to find cache with specified name: " + arg.getCacheName());

            String keyStr = arg.getKey();
            assert keyStr != null;
            assert !keyStr.isEmpty();

            VisorObjectType type = VisorObjectType.parse(arg.getType());

            Object key = parseArgumentValue(type, keyStr);

            assert key != null;

            ClusterNode node = ignite.affinity(cacheName).mapKeyToNode(key);

            UUID nid = node != null ? node.id() : null;

            Object val = cache.withKeepBinary().get(key);

            return new VisorCacheModifyTaskResult(
                nid,
                val instanceof BinaryObject
                    ? U.compact(((BinaryObject)val).type().typeName())
                    : VisorTaskUtils.compactClass(val),
                val
            );
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheGetValueJob.class, this);
        }
    }

    /**
     * Object field description.
     */
    private static class VisorBinaryFieldDescription {
        /** Field type. */
        private VisorObjectType type;

        /** Field name. */
        private String fldName;

        /** Field value. */
        private Object value;

        /**  */
        public VisorBinaryFieldDescription(JsonNode obj) {
            type = VisorObjectType.parse(obj.get("type").textValue());
            fldName = obj.get("name").textValue();

            value = type == VisorObjectType.BINARY
                ? new VisorBinaryClassDescriptor(obj.get("value"))
                : obj.get("value").asText();
        }

        /**
         * @return Field type.
         */
        public VisorObjectType getType() {
            return type;
        }

        /**
         * @return Field name.
         */
        public String getFldName() {
            return fldName;
        }

        /**
         * @return Field value.
         */
        public Object getValue() {
            return value;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorBinaryFieldDescription.class, this);
        }
    }

    /**
     * Object description.
     */
    private static class VisorBinaryClassDescriptor {
        /** Class name. */
        private String className;

        /** Object fields. */
        private List<VisorBinaryFieldDescription> fields;

        /**  */
        public VisorBinaryClassDescriptor(JsonNode obj) {
            className = obj.get("className").textValue();
            fields = new ArrayList<>();

            Iterator<JsonNode> it;

            for (it = obj.get("fields").elements(); it.hasNext();)
                fields.add(new VisorBinaryFieldDescription(it.next()));
        }

        /**
         * @return Class name.
         */
        public String getClassName() {
            return className;
        }

        /**
         * @return Object fields.
         */
        public List<VisorBinaryFieldDescription> getFields() {
            return Collections.unmodifiableList(fields);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorBinaryClassDescriptor.class, this);
        }
    }
}
