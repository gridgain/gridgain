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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryRawReader;

/**
 * Extended reader interface.
 */
public interface BinaryReaderEx extends BinaryRawReader {
    /**
     * Returns protocol version of current object.
     *
     * @return Protocol version.
     */
    public byte version();

    /**
     * Returns total length of data including schema and raw sections.
     *
     * @return Length of the data.
     */
    public int length();

    /**
     * Returns offset of the very first data section beginning.
     *
     * @return Position in stream.
     */
    public int dataStartOffset();

    /**
     * Returns offset of the raw data section.
     *
     * @return Position in stream.
     */
    public int rawOffset();

    /**
     * Returns length of raw data section.
     *
     * @return Count of bytes.
     */
    public int rawLength();

    /**
     * Returns length of schema data section.
     *
     * @return Count of bytes.
     */
    public int dataLength();

    /**
     * Returns offset of the schema descripion section.
     *
     * @return Position in stream.
     */
    public int footerStartOffset();

    /**
     * Returns length of schema description section.
     *
     * @return Count of bytes.
     */
    public int footerLength();

    /**
     * Returns current object's schema id.
     *
     * @return Id of schema.
     */
    public int schemaId();

    /**
     * Returns current object's type id.
     *
     * @return Id og type.
     */
    public int typeId();

    /**
     * Returns current object's flags.
     *
     * @return Flags.
     */
    public short flags();

    /**
     * Returns current object's class name.
     *
     * @return Class name.
     */
    public String className();
}
