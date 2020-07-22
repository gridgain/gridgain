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
package org.apache.ignite.internal.visor.statistics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MessageStatsTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID nodeId;

    /** */
    private StatisticsType statisticsType;

    /** */
    public MessageStatsTaskArg() {
    }

    /** */
    public MessageStatsTaskArg(UUID nodeId, StatisticsType statisticsType) {
        this.nodeId = nodeId;
        this.statisticsType = statisticsType;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public StatisticsType statisticsType() {
        return statisticsType;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        U.writeEnum(out, statisticsType);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        statisticsType = StatisticsType.fromOrdinal(in.readByte());
    }

    /**
     *
     */
    public enum StatisticsType {
        /** */
        PROCESSING("processing"),

        /** */
        QUEUE_WAITING("waiting");

        private final String name;

        /** Enumerated values. */
        private static final StatisticsType[] VALS = values();

        StatisticsType(String name) {
            this.name = name;
        }

        @Override public String toString() {
            return name;
        }

        @Nullable public static StatisticsType fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }
}
