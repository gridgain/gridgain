/**
 * Copyright (c) 2018-2019 Hewlett-Packard Enterprise Development LP.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * Hewlett-Packard Enterprise Development LP.
 */

package org.apache.ignite.internal.benchmarks.trafgen;

/**
 * This class contains the minimum ignite object stored in binaryobject format. Some optional fields may be added
 * dynamically directly to the ignite binary object, to manage parts and indexes.
 */

public class DAOBinaryObject
{
    /**
     * Name of the record's version field, for retrieval by name using ignite binary object.
     */
    public static final String RECORD_VERSION_NAME = "ivVersionId";
    /**
     * Version ID.
     */
    private int ivVersionId;

    /**
     * Empty constructor for ignite.
     */
    public DAOBinaryObject()
    {
        // empty constructor
    }

    /**
     * Getter for version ID.
     * 
     * @return the record version.
     */
    public int getVersionId()
    {
        return ivVersionId;
    }

    /**
     * Setter for version ID.
     * 
     * @param version
     *            the version ID of the record.
     */
    public void setVersionId(int version)
    {
        ivVersionId = version;
    }
}
