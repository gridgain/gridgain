package org.apache.ignite.internal;

import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;

/**
 * The base feature class.
 */
public interface AbstractThinProtocolFeature {
    /**
     * @return Feature ID.
     */
    int featureId();

    /**
     * @return Feature's name.
     */
    String name();

    /**
     * @return The feature's flag bit index in the byte.
     */
    default int bitIdx() {
        return featureId() & 0x7;
    }

    /**
     * @return The feature's byte index where the features flag is placed.
     */
    default int byteIdx() {
        return featureId() >>> 3;
    }

    /**
     * @param features Features set.
     * @return Byte array representing all supported features by current node.
     * @param <E> Features type (JDBC, thin cli, ODBC)
     */
    static <E extends Enum<E> & AbstractThinProtocolFeature> byte[] features(E[] features) {
        final BitSet set = new BitSet();

        for (AbstractThinProtocolFeature f : features) {
            final int featureBit = f.bitIdx() + (f.byteIdx() << 3);

            assert !set.get(featureBit) : "Duplicate thin clients feature ID found for [" + f.name() + "] having same ID ["
                + featureBit + "]";

            set.set(featureBit);
        }

        return set.toByteArray();
    }

    /**
     * @param <E> Features type (JDBC, thin cli, ODBC)
     */
    static <E extends Enum<E> & AbstractThinProtocolFeature> EnumSet<E> enumSet(byte [] in, E[] values) {
        final BitSet bSet = BitSet.valueOf(in);

        EnumSet<E> set = EnumSet.noneOf(values[0].getDeclaringClass());

        if (in == null)
            return set;

        for (E e : values) {
            if (bSet.get(e.featureId()))
                set.add(e);
        }

        return set;
    }

    /**
     * @param fs0 Features set.
     * @param fs1 Other features set.
     * @return Byte array representing all supported features by both features set.
     */
    static byte[] matchFeatures(byte[] fs0, byte[] fs1) {
        byte[] res = new byte[Math.min(fs0.length, fs1.length)];

        for (int i = 0; i < res.length; ++i)
            res[i] = (byte)(fs0[i] & fs1[i]);

        return res;
    }
}
