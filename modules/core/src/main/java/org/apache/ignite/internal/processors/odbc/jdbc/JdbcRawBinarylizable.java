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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;

/**
 * Interface that allows to implement custom serialization
 * logic to raw binary streams.
 */
public interface JdbcRawBinarylizable {
    /**
     * Writes fields to provided writer.
     *
     * @param writer Binary object writer.
     * @param protoCtx JDBC protocol context.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx) throws BinaryObjectException;

    /**
     * Reads fields from provided reader.
     *
     * @param reader Binary object reader.
     * @param protoCtx JDBC protocol context.
     * @throws BinaryObjectException In case of error.
     */
    public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx) throws BinaryObjectException;
}