package org.apache.ignite.lang;

public class IgniteBiVectorTuple<K, V> extends IgniteBiTuple<K, V> {

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

}