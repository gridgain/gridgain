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

package org.apache.ignite.console.utils;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Utilities.
 */
public class Utils {
    /** */
    private static final JsonObject EMPTY_OBJ = new JsonObject();

    /** */
    private static final GridJettyObjectMapper MAPPER = new GridJettyObjectMapper();

    /**
     * Private constructor for utility class.
     */
    private Utils() {
        // No-op.
    }

    /**
     * @param v Value to serialize.
     * @return JSON value.
     * @throws IllegalStateException If serialization failed.
     */
    public static String toJson(Object v) {
        try {
            return MAPPER.writeValueAsString(v);
        }
        catch (Throwable e) {
            throw new IllegalStateException("Failed to serialize as JSON: " + v, e);
        }
    }

    /**
     * Cast object to JSON.
     *
     * @param v Object.
     * @return JSON object.
     */
    public static JsonObject asJson(Object v) {
        if (v instanceof  JsonObject)
            return (JsonObject)v;

        if (v instanceof Map)
            return new JsonObject((Map)v);

        throw new ClassCastException("Not a JSON");
    }

    /**
     * @param json JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(String json, Class<T> cls) throws IOException {
        return MAPPER.readValue(json, cls);
    }

    /**
     * @param json JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(byte[] json, Class<T> cls) throws IOException {
        return MAPPER.readValue(json, cls);
    }

    /**
     * @param src source of JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(Reader src, Class<T> cls) throws IOException {
        return MAPPER.readValue(src, cls);
    }

    /**
     * @param json JSON.
     * @param typeRef Type descriptor.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(String json, TypeReference<T> typeRef) throws IOException {
        return MAPPER.readValue(json, typeRef);
    }

    /**
     * @param json JSON.
     * @return Map with parameters.
     * @throws IllegalStateException If deserialization failed.
     */
    public static JsonObject fromJson(String json) {
        try {
            return MAPPER.readValue(json, JsonObject.class);
        }
        catch (Throwable e) {
            throw new IllegalStateException("Failed to deserialize object from JSON: " + json, e);
        }
    }

    /**
     * Helper method to get attribute.
     *
     * @param attrs Map with attributes.
     * @param name Attribute name.
     * @return Attribute value.
     */
    public static <T> T attribute(Map<String, Object> attrs, String name) {
        return (T)attrs.get(name);
    }

    /**
     * @param cause Error.
     * @return Error message or exception class name.
     */
    public static String errorMessage(Throwable cause) {
        String msg = cause.getMessage();

        return F.isEmpty(msg) ? cause.getClass().getName() : msg;
    }

    /**
     * @param a First set.
     * @param b Second set.
     * @return Elements exists in a and not in b.
     */
    public static TreeSet<UUID> diff(TreeSet<UUID> a, TreeSet<UUID> b) {
        return a.stream().filter(item -> !b.contains(item)).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * @param json JSON object.
     * @param key Key with IDs.
     * @return Set of IDs.
     */
    public static TreeSet<UUID> idsFromJson(JsonObject json, String key) {
        TreeSet<UUID> res = new TreeSet<>();

        JsonArray ids = json.getJsonArray(key);

        if (ids != null)
            ids.forEach(item -> res.add(UUID.fromString(item.toString())));

        return res;
    }

    /**
     * @param json JSON to travers.
     * @param path Dot separated list of properties.
     * @return Tuple with unwind JSON and key to extract from it.
     */
    private static T2<JsonObject, String> xpath(JsonObject json, String path) {
        String[] keys = path.split("\\.");

        for (int i = 0; i < keys.length - 1; i++) {
            json = json.getJsonObject(keys[i]);

            if (json == null)
                json = EMPTY_OBJ;
        }

        String key = keys[keys.length - 1];

        if (json.containsKey(key))
            return new T2<>(json, key);

        throw new IllegalStateException("Parameter not found: " + path);
    }

    /**
     * @param json JSON object.
     * @param path Dot separated list of properties.
     * @param def Default value.
     * @return the value or {@code def} if no entry present.
     */
    public static boolean boolParam(JsonObject json, String path, boolean def) {
        T2<JsonObject, String> t = xpath(json, path);

        return t.getKey().getBoolean(t.getValue(), def);
    }

    /**
     * @param prefix Message prefix.
     * @param e Exception.
     */
    public static String extractErrorMessage(String prefix, Throwable e) {
        String causeMsg = F.isEmpty(e.getMessage()) ? e.getClass().getName() : e.getMessage();

        return prefix + ": " + causeMsg;
    }
}
