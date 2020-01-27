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

package org.apache.ignite.internal;

import java.util.BitSet;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Defines supported features for thin clients (JDBC, ODBC, thin client) and node.
 */
public class ThinClientFeatures {
    /** Features. */
    private final byte [] features;

    /**
     * @param features Supported features.
     */
    public ThinClientFeatures(byte [] features) {
        this.features = features;
    }

    /**
     * @param feature The feature to chek support.
     * @return {@code True} if feature is declared to be supported.
     */
    <E extends Feature> boolean supports(E feature) {
        return supports(features, feature);
    }

    /**
     * @param features Features set.
     * @return Byte array representing all supported features by current node.
     */
    public static <E extends Feature> byte[] features(Collection<E> features) {
        final BitSet set = new BitSet();

        for (Feature f : features) {
            final int featureBit = f.bitIdx + (f.byteIdx << 3);

            assert !set.get(featureBit) : "Duplicate thin clients feature ID found for [" + f.name() + "] having same ID ["
                + featureBit + "]";

            set.set(featureBit);
        }

        return set.toByteArray();
    }

    /**
     * Checks that feature supported by node.
     *
     * @param featuresAttrBytes Byte array value of supported features.
     * @param feature Feature to check.
     * @return {@code True} if feature is declared to be supported.
     */
    public static <E extends Feature> boolean supports(byte[] featuresAttrBytes, E feature) {
        if (featuresAttrBytes == null)
            return false;

        if (feature.byteIdx() >= featuresAttrBytes.length)
            return false;

        return (featuresAttrBytes[feature.byteIdx()] & (1 << feature.bitIdx())) != 0;
    }

    /**
     * @param fs0 Features set.
     * @param fs1 Other features set.
     * @return Byte array representing all supported features by both features set.
     */
    public static byte[] matchFeatures(byte[] fs0, byte[] fs1) {
        byte[] res = new byte[Math.min(fs0.length, fs1.length)];

        for (int i = 0; i < res.length; ++i)
            res[i] = (byte)(fs0[i] & fs1[i]);

        return res;
    }

    /**
     * @param features features set.
     * @param f feature to register.
     */
    public static <E extends Feature> void register(Set<E> features, E f) {
        boolean res = features.add(f);

        assert res : "Duplicate thin clients feature ID found for [" + f.name() + "] having same ID";
    }

    /**
     *
     */
    public static class Feature {
        /** Feature id. */
        private final int byteIdx;

        /** Feature id. */
        private final int bitIdx;

        /** Name. */
        private final String name;

        /**
         * @param featureId Feature Id.
         * @param name Feature name.
         */
        public Feature (int featureId, String name) {
            this.byteIdx = featureId >>> 3;
            this.bitIdx = featureId & 0x7;
            this.name = name;
        }

        /**
         * @return Feature's name.
         */
        public String name() {
            return name;
        }

        /**
         * @return
         */
        int bitIdx() {
            return bitIdx;
        }

        /**
         * @return
         */
        int byteIdx() {
            return byteIdx;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Feature feature = (Feature)o;
            return byteIdx == feature.byteIdx &&
                bitIdx == feature.bitIdx;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(byteIdx, bitIdx);
        }
    }
}
