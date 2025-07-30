package org.apache.ignite.internal.benchmarks.trafgen;

/**
 * A class to hold the tag value. We do not need the value in the tags cache, but we have to give one to ignite.
 */
public class DAOTagValue
{
    /**
     * A dummy byte value.
     */
    private byte value;

    /**
     * Empty constructor for ignite.
     */
    public DAOTagValue()
    {
        value = 0;
    }

    /**
     * Gets the value.
     * 
     * @return the value
     */
    public byte getValue()
    {
        return value;
    }
}
