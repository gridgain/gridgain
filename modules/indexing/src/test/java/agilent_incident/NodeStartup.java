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

package agilent_incident;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import javax.cache.Cache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;

/**
 *
 */
public class NodeStartup {
    /**
     *
     */
    static int nodeCnt = 0;

    /**
     *
     */
    static FileSystem FS = FileSystems.getDefault();

    /**
     *
     */
    static Path workDir = FS.getPath("/home/tledkov/agl-wrk");

    /**
     *
     */
    static IgniteConfiguration cfg = new IgniteConfiguration()
        .setIgniteInstanceName("agl-dbg")
        .setConsistentId("agl-dbg" + nodeCnt++)
        .setCacheConfiguration(
            new CacheConfiguration()
                .setName("Agilent_Replicated")
                .setCacheMode(CacheMode.REPLICATED)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setQueryDetailMetricsSize(100)
        )
        .setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        )
        .setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(
                    new TcpDiscoveryVmIpFinder()
                        .setAddresses(Arrays.asList("127.0.0.1:47500..47510"))
                )
        )
        .setWorkDirectory(workDir.toString());

    /**
     *
     */
    public static void main(String[] args) throws Exception {
        System.setProperty(IGNITE_SENSITIVE_DATA_LOGGING, "plain");
//        initialStartup();

        dbg();
    }

    /**
     *
     */
    public static void initialStartup() throws IgniteInterruptedCheckedException {
        try (IgniteEx ign = (IgniteEx)Ignition.start(cfg)) {
            ign.cluster().state(ClusterState.ACTIVE);

            ign.context().query().querySqlFields(
                new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES ( " +
                    "ASSET_ID VARCHAR, " +
                    "ATTRIBUTE_ID VARCHAR, " +
                    "LANGUAGE VARCHAR, " +
                    "ATTRIBUTE_VALUE VARCHAR, " +
                    "ATTRIBUTES_STRING VARCHAR, " +
                    "IS_MULTIPLE_VALUE VARCHAR, " +
                    "CREATE_TIME TIMESTAMP, " +
                    "MODIFY_TIME TIMESTAMP, " +
                    "STATUS VARCHAR, " +
                    "CONSTRAINT PK_PUBLIC_PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES PRIMARY KEY (ASSET_ID,ATTRIBUTE_ID,LANGUAGE,ATTRIBUTE_VALUE)\n" +
                    ") WITH \"" +
                    "template=REPLICATED, " +
                    "key_type=WCSKeyType10, " +
                    "CACHE_NAME=WCSC10, " +
                    "value_type=WCSValueType10" +
                    "\""
                ),
                false);

            ign.context().query().querySqlFields(
                new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS " +
                    "PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES_IDX_ASSET_ID_LANGUAGE " +
                    "ON PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES (ASSET_ID, LANGUAGE)"
                ),
                false);

            System.out.println("+++ READY");

            U.sleep(60_000);
        }
    }

    /**
     *
     */
    public static void dbg() throws IgniteInterruptedCheckedException {
        try (IgniteEx ign = (IgniteEx)Ignition.start(cfg)) {
            ign.cluster().state(ClusterState.ACTIVE);

//            U.sleep(Long.MAX_VALUE);

            while (true) {
                try {
//                    FieldsQueryCursor<List<?>> cur = ign.context().query().querySqlFields(new SqlFieldsQuery(
//                        "SELECT * FROM PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES AS AA " +
//                            " LEFT JOIN (SELECT * FROM PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES USE INDEX(PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES_IDX_ASSET_ID_LANGUAGE)) AS IDX " +
//                            "ON AA.ASSET_ID=IDX.ASSET_ID " +
//                            "AND AA.ATTRIBUTE_ID=IDX.ATTRIBUTE_ID " +
//                            "AND AA.LANGUAGE=IDX.LANGUAGE " +
//                            "AND AA.ATTRIBUTE_VALUE=IDX.ATTRIBUTE_VALUE"), true);

//                    FieldsQueryCursor<List<?>> cur = ign.context().query().querySqlFields(new SqlFieldsQuery(
//                        "SELECT * FROM PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES"), true);
//
//                    int cnt = 0;
//                    for (List<?> r : cur) {
//                        System.out.println("+++" + r);
//                        cnt++;
//                    }
//
//                    System.out.println("cnt=" + cnt);
//                    System.out.println("size=" + ign.cache("WCSC10").size());

                    QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> cur =
                        ign.cache("WCSC10").withKeepBinary().query(new ScanQuery<>());

                    int example = 0;
                    for (Cache.Entry<BinaryObject, BinaryObject> e : cur) {
                        List<List<?>> res = ign.context().query().querySqlFields(new SqlFieldsQuery(
                            "SELECT * FROM PIM_WCS_ASSET_DYNAMIC_ATTRIBUTES WHERE " +
                            "ASSET_ID=? " +
                            "AND ATTRIBUTE_ID=? " +
                            "AND LANGUAGE=? " +
                            "AND ATTRIBUTE_VALUE=?")
                            .setArgs(
                                e.getKey().field("ASSET_ID"),
                                e.getKey().field("ATTRIBUTE_ID"),
                                e.getKey().field("LANGUAGE"),
                                e.getKey().field("ATTRIBUTE_VALUE")
                                ), true).getAll();

                        if (res.isEmpty())
                            System.out.println("+++ " + e);
                        else if (example < 10){
                            example++;
//                            System.out.println("--- " + e);
                        }

                    }

                    return;
                }
                catch (Exception e) {
                    e.printStackTrace();

                    U.sleep(10_000);
                }
            }
        }
    }
}
