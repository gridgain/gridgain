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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ThinProtocolFeature;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Defines supported features for JDBC thin client.
 */
public enum JdbcThinFeature implements ThinProtocolFeature {
    /** */
    RESERVED(0);

    /** */
    private static final EnumSet<JdbcThinFeature> ALL_FEATURES_AS_ENUM_SET = EnumSet.allOf(JdbcThinFeature.values()[0].getDeclaringClass());

    /** */
    private static final byte[] ALL_FEATURES_AS_BYTES;

    static {
        List<JdbcThinFeature> values = Arrays.stream(JdbcThinFeature.values())
            .sorted((f1, f2) -> Integer.compare(f2.featureId(), f1.featureId()))
            .collect(Collectors.toList());

        int maxBytesNo = values.get(0).featureId() >>> 3;

        ALL_FEATURES_AS_BYTES = new byte[maxBytesNo + 1];

        for (JdbcThinFeature f : values) {
            int byteNo = f.featureId() >>> 3;
            int bitNo = f.featureId() & 0x7;

            ALL_FEATURES_AS_BYTES[byteNo] |= bitNo;
        }
    }

    /** Feature id. */
    private final int featureId;

    /**
     * @param id Feature ID.
     */
    JdbcThinFeature(int id) {
        featureId = id;
    }

    /** {@inheritDoc} */
    @Override public int featureId() {
        return featureId;
    }

    /**
     * @param bytes Byte array representing supported features.
     * @return {@code true} if the feature is enabled at the set specified by bytes array.
     */
    private boolean isFeatureSet(byte[] bytes) {
        int byteNo = featureId >>> 3;
        int bitNo = featureId & 0x7;

        return bytes != null && bytes.length > byteNo && ((bytes[byteNo] & bitNo) != 0);
    }

    /** */
    public static EnumSet<JdbcThinFeature> enumSet(byte[] bytes) {
        EnumSet<JdbcThinFeature> set = EnumSet.noneOf(JdbcThinFeature.values()[0].getDeclaringClass());

        if (F.isEmpty(bytes))
            return set;

        for (JdbcThinFeature f : JdbcThinFeature.values()) {
            if (f.isFeatureSet(bytes))
                set.add(f);
        }

        return set;
    }

    /** */
    public static EnumSet<JdbcThinFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }

    /**
     * @return Byte array representing all supported features.
     */
    public static byte[] allFeaturesAsBytes() {
        return Arrays.copyOf(ALL_FEATURES_AS_BYTES, ALL_FEATURES_AS_BYTES.length);
    }
}
