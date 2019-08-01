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
package org.h2.expression.aggregate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.h2.value.Value;

/**
 * TODO: Add class description.
 */
public class AggregateSerializer {
    private static final int NULL = 0;
    private static final int COUNT = 1;
    private static final int DEFAULT = 2;

    private AggregateSerializer() {
    }

    public static void writeAggregate(Object agg, DataOutputStream out) throws IOException {
        if (!canSpillToDisk(agg))
            throw new RuntimeException("Aggregate can not be serialized." + agg.getClass());

        if (agg == null) {
            out.writeInt(NULL);
        }
        else if (agg instanceof AggregateDataCount) {
            AggregateDataCount aggCnt = (AggregateDataCount)agg;

            out.writeInt(COUNT);

            aggCnt.write(out);
        }
        else if (agg instanceof AggregateDataDefault) {
            AggregateDataDefault aggDflt = (AggregateDataDefault)agg;

            out.writeInt(DEFAULT);

            aggDflt.write(out);
        }
        else
            throw new UnsupportedOperationException("Can not serialize aggregate: " + agg);

    }

    public static Object readAggregate(DataInputStream in) throws IOException {
        int type = in.readInt();

        switch (type) {
            case NULL:
                return null;

            case COUNT:
                return AggregateDataCount.read(in);

            case DEFAULT:
                return AggregateDataDefault.read(in);

            default:
                throw new UnsupportedOperationException("Unknown aggregate type: " + type);
        }

    }

    private static boolean canSpillToDisk(Object agg) {
        if (agg == null)
            return true;

        if (agg instanceof AggregateData)
            return ((AggregateData)agg).hasFixedSizeInBytes(); // Not all children of AggregateData can be spilled to disk.

        if (agg instanceof org.h2.api.Aggregate)
            return false; // We can not spill user-defined aggregates.

        assert agg instanceof Value : agg.getClass();

        return ((Value)agg).hasFixedSizeInBytes(); // At the moment values with the fixed size can be spilled to disk.
    }
}
