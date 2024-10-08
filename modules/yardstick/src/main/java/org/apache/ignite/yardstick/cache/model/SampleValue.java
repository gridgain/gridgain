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

package org.apache.ignite.yardstick.cache.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Entity class for benchmark.
 */
public class SampleValue implements Externalizable, Binarylizable {
    /** */
    @QuerySqlField
    private int id;

    /** */
    public SampleValue() {
        // No-op.
    }

    /**
     * @param id Id.
     */
    public SampleValue(int id) {
        this.id = id;
    }

    /**
     * @param id Id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return Id.
     */
    public int getId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        id = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        writer.writeInt("id", id);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        id = reader.readInt("id");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "SampleValue [id=" + id + ']';
    }
}