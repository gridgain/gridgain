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

package org.apache.ignite.internal.pagemem.wal.record;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Interface for Data Entry for automatic unwrapping key and value from Data Entry
 */
public interface UnwrappedDataEntry {
    /**
     * Unwraps key value from cache key object into primitive boxed type or source class. If client classes were used in
     * key, call of this method requires classes to be available in classpath.
     *
     * @return Key which was placed into cache. Or null if failed to convert.
     */
    Object unwrappedKey();

    /**
     * Unwraps value value from cache value object into primitive boxed type or source class. If client classes were
     * used in key, call of this method requires classes to be available in classpath.
     *
     * @return Value which was placed into cache. Or null for delete operation or for failure.
     */
    Object unwrappedValue();

    /**
     * Returns a string representation of the entry.
     *
     * @param entry          Object to get a string presentation for.
     * @param superToString  String representation of parent.
     * @param cacheObjValCtx Cache object value context. Context is used for unwrapping objects.
     * @param <T>            Composite type: extends DataEntry implements UnwrappedDataEntry
     * @return String presentation of the given object.
     */
    public static <T extends DataEntry & UnwrappedDataEntry> String toString(
        T entry,
        String superToString,
        final CacheObjectValueContext cacheObjValCtx
    ) {
        final String keyStr= toString(entry, false, cacheObjValCtx);

        final String valueStr= toString(entry, true, cacheObjValCtx);

        return entry.getClass().getSimpleName() + "[k = " + keyStr + ", v = ["
            + valueStr
            + "], super = ["
            + superToString + "]]";
    }

    /**
     * Returns a string representation of the entry key or entry value.
     *
     * @param entry          Object to get a string presentation for.
     * @param isValue  If {@code true} then function return string representation of the entry value else entry key.
     * @param cacheObjValCtx Cache object value context. Context is used for unwrapping objects.
     * @param <T>            Composite type: extends DataEntry implements UnwrappedDataEntry
     * @return String presentation of the entry key or entry value depends on {@code isValue}.
     */
    public static <T extends DataEntry & UnwrappedDataEntry> String toString(T entry,
        boolean isValue,
        final CacheObjectValueContext cacheObjValCtx
    ) {
        final Object value;
        if (isValue)
            value = entry.unwrappedValue();
        else
            value = entry.unwrappedKey();

        String str;
        if (value instanceof String)
            str = (String)value;
        else if (value instanceof BinaryObject)
            str = value.toString();
        else
            str = (value != null) ? toStringRecursive(value.getClass(), value) : null;

        if (str == null || str.isEmpty()) {
            final CacheObject co;

            if (isValue)
                co = entry.value();
            else
                co = entry.key();

            try {
                str = Base64.getEncoder().encodeToString(co.valueBytes(cacheObjValCtx));
            }
            catch (IgniteCheckedException e) {
                cacheObjValCtx.kernalContext().log(UnwrapDataEntry.class)
                    .error("Unable to convert " + (isValue ? "value" : "key") + " [" + co + "]", e);
            }
        }

        if (!S.includeSensitive()) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");

                byte[] hash = digest.digest(str.getBytes(StandardCharsets.UTF_8));

                StringBuffer hexString = new StringBuffer();

                for (int i = 0; i < hash.length; i++) {
                    String hex = Integer.toHexString(0xff & hash[i]);

                    if (hex.length() == 1)
                        hexString.append('0');

                    hexString.append(hex);
                }

                str = hexString.toString();
            }
            catch (NoSuchAlgorithmException e) {
                cacheObjValCtx.kernalContext().log(UnwrapDataEntry.class)
                    .error(e.getMessage(), e);
            }
        }

        return str;
    }

    /**
     * Produces auto-generated output of string presentation for given object (given the whole hierarchy).
     *
     * @param cls Declaration class of the object.
     * @param obj Object to get a string presentation for.
     * @return String presentation of the given object.
     */
    public static String toStringRecursive(Class cls, Object obj) {
        String result = null;

        if (cls != Object.class)
            result = S.toString(cls, obj, toStringRecursive(cls.getSuperclass(), obj));

        return result;
    }
}
