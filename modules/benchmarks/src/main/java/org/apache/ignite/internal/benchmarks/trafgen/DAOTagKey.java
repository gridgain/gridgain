package org.apache.ignite.internal.benchmarks.trafgen;

import java.io.Serializable;

/**
 * This class implements the composite key for a Tags cache related to a String-keyed data cache.
 * <p>
 * This key is composed of the tag name, the tag value and the String key (still defined as a Serializable for
 * backward compatibility reasons).
 * <p>
 */
public class DAOTagKey implements Serializable
{
    private static final long serialVersionUID = -3805288710372656110L;

    /**
     * Constant for the field name for tagName. Used for building ignite index.
     */
    public static final String TAGNAME_FIELD_NAME = "tagName";
    private String tagName;

    /**
     * Constant for the field name for tagValue. Used for building ignite index.
     */
    public static final String TAGVALUE_FIELD_NAME = "tagValue";
    private String tagValue;

    /**
     * Constant for the field name for key. Used for building the ignite queryentity.
     */
    public static final String KEY_FIELD_NAME = "key";
    private Serializable key;

    /**
     * Constructor.
     */
    public DAOTagKey()
    {
    }

    /**
     * Constructor.
     * 
     * @param name
     *            tag name
     * @param value
     *            tag value
     * @param akey
     *            the key object (serializable)
     */
    public DAOTagKey(String name, String value, Serializable akey)
    {
        tagName = name;
        tagValue = value;
        key = akey;
    }

    /**
     * set the tag name.
     * 
     * @param name
     *            the name
     */
    public void setTagName(String name)
    {
        tagName = name;
    }

    /**
     * Get the tag name.
     * 
     * @return the tag name
     */
    public String getTagName()
    {
        return tagName;
    }

    /**
     * set the tag value.
     * 
     * @param value
     *            the tag value
     */
    public void setTagValue(String value)
    {
        tagValue = value;
    }

    /**
     * get the tag value.
     * 
     * @return the tag value.
     */
    public String getTagValue()
    {
        return tagValue;
    }

    /**
     * Set the tag key.
     * 
     * @param akey
     *            the tag akey
     */
    public void setKey(Serializable akey)
    {
        key = akey;
    }

    public final Serializable getKey()
    {
        return key;
    }
}
