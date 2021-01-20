/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.util.typedef.F;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** */
public class IgniteStatisticsSchemaRepository {
    /** */
    private static final String STAT_OBJ_PREFIX = "sql.statobj.";

    /** Logger. */
    private final IgniteLogger log;

    /** Statistics store. */
    private final DistributedMetaStorage distrMetaStorage;

    /** */
    public IgniteStatisticsSchemaRepository(
        DistributedMetaStorage distrMetaStorage,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.distrMetaStorage = distrMetaStorage;
        this.log = logSupplier.apply(IgniteStatisticsSchemaRepository.class);
    }

    /** */
    private static String key2String(StatisticsKey key) {
        return key.schema() + "." + key.obj();
    }


    /** */
    public static class StatisticInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private StatisticsKey key;

        /** */
        private StatisticColumnInfo[] cols;
    }

    /** */
    public static class StatisticColumnInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String name;

        /** */
        private StatisticColumnState state;
    }

    /** */
    enum StatisticColumnState {
        /** */
        READY,

        /** */
        IN_PROGRESS
    };

}
