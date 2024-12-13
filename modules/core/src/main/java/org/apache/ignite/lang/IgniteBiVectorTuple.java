package org.apache.ignite.lang;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

public class IgniteBiVectorTuple<K, V> extends IgniteBiTuple<K, V> implements Binarylizable {

    private static final long serialVersionUID = 0L;
    private double score;

    /**
     * Empty constructor required for binary deserialization.
     */
    public IgniteBiVectorTuple() {
        // No-op
    }

    /**
     * Creates vector query tuple with the specified key, value and score.
     *
     * @param key Key.
     * @param value Value.
     * @param score Score.
     */
    public IgniteBiVectorTuple(K key, V value, double score) {
        super(key, value);
        this.score = score;
    }

    /**
     * Gets score.
     *
     * @return Score.
     */
    public double getScore() {
        return score;
    }

    /**
     * Sets score.
     *
     * @param score Score.
     */
    public void setScore(double score) {
        this.score = score;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeObject("key", getKey());
        writer.writeObject("value", getValue());
        writer.writeDouble("score", score);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        set1(reader.readObject("key"));
        set2(reader.readObject("value"));
        score = reader.readDouble("score");
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return String.format("VectorQueryResult{key=%s, value=%s, score=%f}",
                getKey(), getValue(), score);
    }

    /**
     * Static factory method to convert IgniteBiTuple to VectorQueryResult.
     *
     * @param tuple Base tuple.
     * @param score Score.
     * @return Vector query result.
     */
    public static <K, V> IgniteBiVectorTuple<K, V> from(
            IgniteBiTuple<K, V> tuple,
            double score) {
        return new IgniteBiVectorTuple<>(tuple.getKey(), tuple.getValue(), score);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        long temp = Double.doubleToLongBits(score);
        result = prime * result + (int)(temp ^ (temp >>> 32));
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        IgniteBiVectorTuple<?, ?> other = (IgniteBiVectorTuple<?, ?>) obj;
        return Double.doubleToLongBits(score) == Double.doubleToLongBits(other.score);
    }
}