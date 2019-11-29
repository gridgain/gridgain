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

import IgniteJavaTransformer from './JavaTransformer.service';
import IgniteClusterDefaults from './defaults/Cluster.service'
import {assert} from 'chai';
import {outdent} from 'outdent/lib';

suite('Java transformer tests', () => {
    const caches = [{
        name: 'testCache'
    }];

    const testPkg = 'org.test';
    const testClass = 'TestClassName';
    const cfgFileName = '"TestCfg.xml"';

    const startMarker = `public class ${testClass}`;

    const _generate = (persistenceEnabled: boolean) => {
        const generated = IgniteJavaTransformer.loadCaches(caches, testPkg, testClass, cfgFileName, persistenceEnabled);

        return generated.substring(generated.indexOf(startMarker));
    };

    test('Cluster activation for cluster with persistence', () => {
        assert.equal(_generate(false),
            outdent`
                public class TestClassName {
                    /**
                     * <p>
                     * Utility to load caches from database.
                     * <p>
                     * How to use:
                     * <ul>
                     *     <li>Start cluster.</li>
                     *     <li>Start this utility and wait while load complete.</li>
                     * </ul>
                     * 
                     * @param args Command line arguments, none required.
                     * @throws Exception If failed.
                     **/
                    public static void main(String[] args) throws Exception {
                        try (Ignite ignite = Ignition.start("TestCfg.xml")) {
                            System.out.println(">>> Loading caches...");
                
                            System.out.println(">>> Loading cache: testCache");
                            ignite.cache("testCache").loadCache(null);
                
                            System.out.println(">>> All caches loaded!");
                        }
                    }
                }
                `
        );

        assert.equal(_generate(true),
            outdent`
                public class TestClassName {
                    /**
                     * <p>
                     * Utility to load caches from database.
                     * <p>
                     * How to use:
                     * <ul>
                     *     <li>Start cluster.</li>
                     *     <li>Start this utility and wait while load complete.</li>
                     * </ul>
                     * 
                     * @param args Command line arguments, none required.
                     * @throws Exception If failed.
                     **/
                    public static void main(String[] args) throws Exception {
                        try (Ignite ignite = Ignition.start("TestCfg.xml")) {
                            ignite.cluster().active(true);
                
                            System.out.println(">>> Loading caches...");
                
                            System.out.println(">>> Loading cache: testCache");
                            ignite.cache("testCache").loadCache(null);
                
                            System.out.println(">>> All caches loaded!");
                        }
                    }
                }
                `
        );
    });

    test('Should generate valid list of imports', () => {
        const clusterDflts = new IgniteClusterDefaults();

        const TEST_CONFIGURATION = {
            properties: [{
                clsName: "java.lang.String",
                name: "igniteInstanceName",
                value: "Cluster1"
            }, {
                clsName: "BEAN",
                name: "discoverySpi",
                value: {
                    properties: [{
                        clsName: "BEAN",
                        name: "ipFinder",
                        value: {
                            properties: [{
                                id: "addrs",
                                name: "addresses",
                                items: ["127.0.0.1:47500..47510"],
                                clsName: "COLLECTION",
                                typeClsName: "java.lang.String",
                                implClsName: "java.util.ArrayList"
                            }],
                            arguments: [],
                            clsName: "org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder",
                            id: "ipFinder",
                            src: {
                                addresses: ["127.0.0.1:47500..47510"]
                            },
                            dflts: clusterDflts.discovery.Multicast
                        }
                    }],
                    arguments: [],
                    clsName: "org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi",
                    id: "discovery",
                    src: {
                        kind: "Multicast",
                        Vm: {
                            addresses: ["127.0.0.1:47500..47510"]
                        },
                        Multicast: {
                            addresses: ["127.0.0.1:47500..47510"]
                        },
                        Jdbc: {
                            initSchema: true
                        },
                        Cloud: {
                            regions: [],
                            zones: []
                        }
                    },
                    dflts: clusterDflts
                }
            }],
            arguments: [],
            clsName: "org.apache.ignite.configuration.IgniteConfiguration",
            id: "cfg",
            src: {
                id: "c040a565-3fb6-4328-8a87-4cdc4e58e361",
                activeOnStart: true,
                cacheSanityCheckEnabled: true,
                atomicConfiguration: {},
                cacheKeyConfiguration: [],
                deploymentSpi: {
                    URI: {
                        uriList: [],
                        scanners: []
                    }
                },
                marshaller: {},
                peerClassLoadingLocalClassPathExclude: [],
                sslContextFactory: {
                    trustManagers: []
                },
                swapSpaceSpi: {},
                transactionConfiguration: {},
                dataStorageConfiguration: {
                    pageSize: null,
                    concurrencyLevel: null,
                    defaultDataRegionConfiguration: {
                        name: "default"
                    },
                    dataRegionConfigurations: []
                },
                memoryConfiguration: {
                    pageSize: null,
                    memoryPolicies: [{
                        name: "default",
                        maxSize: null
                    }]
                },
                serviceConfigurations: [],
                executorConfiguration: [],
                sqlConnectorConfiguration: {
                    tcpNoDelay: true
                },
                clientConnectorConfiguration: {
                    tcpNoDelay: true,
                    jdbcEnabled: true,
                    odbcEnabled: true,
                    thinClientEnabled: true,
                    useIgniteSslContextFactory: true
                },
                discovery: {
                    kind: "Multicast",
                    Vm: {
                        addresses: ["127.0.0.1:47500..47510"]
                    },
                    Multicast: {
                        addresses: ["127.0.0.1:47500..47510"]
                    },
                    Jdbc: {
                        initSchema: true
                    },
                    Cloud: {
                        regions: [],
                        zones: []
                    }
                },
                binaryConfiguration: {
                    typeConfigurations: [],
                    compactFooter: true
                },
                communication: {
                    tcpNoDelay: true
                },
                connector: {
                    noDelay: true
                },
                collision: {
                    kind: "Noop",
                    JobStealing: {
                        stealingEnabled: true
                    },
                    PriorityQueue: {
                        starvationPreventionEnabled: true
                    }
                },
                failoverSpi: [],
                logger: {
                    Log4j: {
                        mode: "Default"
                    }
                },
                caches: [],
                models: [],
                checkpointSpi: [],
                loadBalancingSpi: [],
                autoActivationEnabled: true,
                name: "Cluster1"
            },
            dflts: clusterDflts
        };

        const EXPECTED_IMPORTS = [
            'org.apache.ignite.configuration.IgniteConfiguration',
            'org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi',
            'org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;',
            'java.util.Arrays',
            '',
            '',
            '',
            ''
        ];
    });

    test('Should generate list of imports for big configuration without exceptions', () => {

    });

    const TEST_TRANSFORMER_DATA = {
        properties: [{clsName: "boolean", name: "clientMode", value: true}, {
            clsName: "java.lang.String",
            name: "igniteInstanceName",
            value: "Cluster"
        }, {
            clsName: "BEAN", name: "discoverySpi", value: {
                properties: [{
                    clsName: "BEAN",
                    name: "ipFinder",
                    value: {
                        properties: [{
                            id: "addrs",
                            name: "addresses",
                            items: ["127.0.0.1:47500..47510"],
                            clsName: "COLLECTION",
                            typeClsName: "java.lang.String",
                            implClsName: "java.util.ArrayList"
                        }],
                        arguments: [],
                        clsName: "org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder",
                        id: "ipFinder",
                        src: {addresses: ["127.0.0.1:47500..47510"]},
                        dflts: {
                            multicastGroup: "228.1.2.4",
                            multicastPort: 47400,
                            responseWaitTime: 500,
                            addressRequestAttempts: 2,
                            localAddress: "0.0.0.0"
                        }
                    }
                }],
                arguments: [],
                clsName: "org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi",
                id: "discovery",
                src: {
                    kind: "Multicast",
                    Vm: {addresses: ["127.0.0.1:47500..47510"]},
                    Multicast: {addresses: ["127.0.0.1:47500..47510"]},
                    Jdbc: {initSchema: true},
                    Cloud: {regions: [], zones: []}
                },
                dflts: {
                    localPort: 47500,
                    localPortRange: 100,
                    socketTimeout: 5000,
                    ackTimeout: 5000,
                    maxAckTimeout: 600000,
                    networkTimeout: 5000,
                    joinTimeout: 0,
                    threadPriority: 10,
                    heartbeatFrequency: 2000,
                    maxMissedHeartbeats: 1,
                    maxMissedClientHeartbeats: 5,
                    topHistorySize: 1000,
                    reconnectCount: 10,
                    statisticsPrintFrequency: 0,
                    ipFinderCleanFrequency: 60000,
                    forceServerMode: false,
                    clientReconnectDisabled: false,
                    reconnectDelay: 2000,
                    connectionRecoveryTimeout: 10000,
                    soLinger: 5,
                    Multicast: {
                        multicastGroup: "228.1.2.4",
                        multicastPort: 47400,
                        responseWaitTime: 500,
                        addressRequestAttempts: 2,
                        localAddress: "0.0.0.0"
                    },
                    Jdbc: {initSchema: false},
                    SharedFs: {path: "disco/tcp"},
                    ZooKeeper: {
                        basePath: "/services",
                        serviceName: "ignite",
                        allowDuplicateRegistrations: false,
                        ExponentialBackoff: {baseSleepTimeMs: 1000, maxRetries: 10},
                        BoundedExponentialBackoffRetry: {
                            baseSleepTimeMs: 1000,
                            maxSleepTimeMs: 2147483647,
                            maxRetries: 10
                        },
                        UntilElapsed: {maxElapsedTimeMs: 60000, sleepMsBetweenRetries: 1000},
                        RetryNTimes: {n: 10, sleepMsBetweenRetries: 1000},
                        OneTime: {sleepMsBetweenRetry: 1000},
                        Forever: {retryIntervalMs: 1000}
                    },
                    Kubernetes: {
                        serviceName: "ignite",
                        namespace: "default",
                        masterUrl: "https://kubernetes.default.svc.cluster.local:443",
                        accountToken: "/var/run/secrets/kubernetes.io/serviceaccount/token"
                    }
                }
            }
        }, {
            clsName: "ARRAY",
            id: "failoverSpi",
            name: "failoverSpi",
            items: [{
                properties: [],
                arguments: [],
                clsName: "org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi",
                id: "failoverSpi",
                dflts: {maximumFailoverAttempts: 5}
            }],
            typeClsName: "org.apache.ignite.spi.failover.FailoverSpi"
        }, {
            clsName: "ARRAY", id: "ccfgs", name: "cacheConfiguration", items: [{
                properties: [{
                    clsName: "java.lang.String",
                    name: "name",
                    value: "Cache"
                }, {
                    clsName: "org.apache.ignite.cache.CacheMode",
                    name: "cacheMode",
                    value: "PARTITIONED"
                }, {
                    clsName: "org.apache.ignite.cache.CacheAtomicityMode",
                    name: "atomicityMode",
                    value: "ATOMIC"
                }, {clsName: "int", name: "copyOnRead", value: true}, {
                    clsName: "BEAN", name: "cacheStoreFactory", value: {
                        properties: [{
                            clsName: "DATA_SOURCE",
                            id: "ds",
                            name: "dataSourceBean",
                            value: {
                                properties: [{
                                    clsName: "PROPERTY",
                                    name: "jdbcUrl",
                                    value: "ds.jdbc.url",
                                    hint: "jdbc:your_database"
                                }, {
                                    clsName: "PROPERTY",
                                    name: "user",
                                    value: "ds.jdbc.username",
                                    hint: "YOUR_USER_NAME"
                                }, {
                                    clsName: "PROPERTY",
                                    name: "password",
                                    value: "ds.jdbc.password",
                                    hint: "YOUR_PASSWORD"
                                }],
                                arguments: [],
                                clsName: "com.mchange.v2.c3p0.ComboPooledDataSource",
                                id: "ds",
                                src: {},
                                dflts: {}
                            }
                        }, {
                            clsName: "BEAN",
                            name: "dialect",
                            value: {
                                properties: [],
                                arguments: [],
                                clsName: "org.apache.ignite.cache.store.jdbc.dialect.BasicJdbcDialect"
                            }
                        }, {
                            clsName: "ARRAY", id: "types", name: "types", items: [{
                                properties: [{
                                    clsName: "java.lang.String",
                                    name: "cacheName",
                                    value: "Cache"
                                }, {
                                    clsName: "java.lang.Class",
                                    name: "keyType",
                                    value: "BigDecimal"
                                }, {
                                    clsName: "java.lang.String",
                                    name: "valueType",
                                    value: "test.ClassName"
                                }, {
                                    clsName: "java.lang.String",
                                    name: "databaseSchema",
                                    value: "schema"
                                }, {
                                    clsName: "java.lang.String",
                                    name: "databaseTable",
                                    value: "table"
                                }, {
                                    clsName: "ARRAY",
                                    id: "keyFields",
                                    name: "keyFields",
                                    items: [{
                                        properties: [],
                                        arguments: [{
                                            clsName: "java.sql.Types",
                                            constant: true,
                                            value: "NUMERIC"
                                        }, {
                                            clsName: "java.lang.String",
                                            name: null,
                                            value: "ID"
                                        }, {
                                            clsName: "java.lang.Class",
                                            name: null,
                                            value: "Integer"
                                        }, {clsName: "java.lang.String", name: null, value: "id"}],
                                        clsName: "org.apache.ignite.cache.store.jdbc.JdbcTypeField",
                                        id: "typeField",
                                        src: {
                                            databaseFieldName: "ID",
                                            databaseFieldType: "NUMERIC",
                                            javaFieldName: "id",
                                            javaFieldType: "Integer"
                                        },
                                        dflts: {databaseFieldType: {clsName: "java.sql.Types"}}
                                    }],
                                    typeClsName: "org.apache.ignite.cache.store.jdbc.JdbcTypeField",
                                    varArg: true
                                }, {
                                    clsName: "ARRAY",
                                    id: "valueFields",
                                    name: "valueFields",
                                    items: [{
                                        properties: [],
                                        arguments: [{
                                            clsName: "java.sql.Types",
                                            constant: true,
                                            value: "VARCHAR"
                                        }, {
                                            clsName: "java.lang.String",
                                            name: null,
                                            value: "VALUE"
                                        }, {
                                            clsName: "java.lang.Class",
                                            name: null,
                                            value: "Date"
                                        }, {clsName: "java.lang.String", name: null, value: "value"}],
                                        clsName: "org.apache.ignite.cache.store.jdbc.JdbcTypeField",
                                        id: "typeField",
                                        src: {
                                            databaseFieldName: "VALUE",
                                            databaseFieldType: "VARCHAR",
                                            javaFieldName: "value",
                                            javaFieldType: "Date"
                                        },
                                        dflts: {databaseFieldType: {clsName: "java.sql.Types"}}
                                    }],
                                    typeClsName: "org.apache.ignite.cache.store.jdbc.JdbcTypeField",
                                    varArg: true
                                }],
                                arguments: [],
                                clsName: "org.apache.ignite.cache.store.jdbc.JdbcType",
                                id: "type",
                                src: {
                                    id: "d1164cda-f843-4f2d-b0d0-0ce4f0bc2d15",
                                    generatePojo: true,
                                    caches: ["65743c09-0bcb-48b0-b1c6-88b36b22f2ac"],
                                    queryKeyFields: ["id"],
                                    queryMetadata: "Configuration",
                                    name: "",
                                    keyType: "BigDecimal",
                                    valueType: "test.ClassName",
                                    tableName: "",
                                    fields: [{
                                        name: "id",
                                        className: "Integer",
                                        notNull: true
                                    }, {name: "value", className: "Timestamp"}],
                                    indexes: [],
                                    databaseSchema: "schema",
                                    databaseTable: "table",
                                    keyFields: [{
                                        databaseFieldName: "ID",
                                        databaseFieldType: "NUMERIC",
                                        javaFieldName: "id",
                                        javaFieldType: "Integer"
                                    }],
                                    valueFields: [{
                                        databaseFieldName: "VALUE",
                                        databaseFieldType: "VARCHAR",
                                        javaFieldName: "value",
                                        javaFieldType: "Date"
                                    }],
                                    cacheName: "Cache"
                                },
                                dflts: {}
                            }], typeClsName: "org.apache.ignite.cache.store.jdbc.JdbcType", varArg: true
                        }],
                        arguments: [],
                        clsName: "org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory",
                        id: "cacheStoreFactory",
                        src: {dataSourceBean: "ds", dialect: "Generic"},
                        dflts: {
                            batchSize: 512,
                            maximumWriteAttempts: 2,
                            parallelLoadCacheMinimumThreshold: 512,
                            sqlEscapeAll: false
                        }
                    }
                }, {clsName: "boolean", name: "readThrough", value: true}, {
                    clsName: "boolean",
                    name: "writeThrough",
                    value: true
                }, {clsName: "boolean", name: "eagerTtl", value: true}, {
                    id: "qryEntities",
                    name: "queryEntities",
                    items: [{
                        properties: [{
                            clsName: "java.lang.String",
                            name: "keyType",
                            value: "java.math.BigDecimal"
                        }, {
                            clsName: "java.lang.String",
                            name: "valueType",
                            value: "test.ClassName"
                        }, {
                            id: "keyFields",
                            name: "keyFields",
                            items: ["id"],
                            clsName: "COLLECTION",
                            typeClsName: "java.lang.String",
                            implClsName: "java.util.HashSet"
                        }, {
                            clsName: "MAP",
                            id: "fields",
                            name: "fields",
                            ordered: true,
                            keyClsName: "java.lang.String",
                            keyField: "name",
                            valClsName: "java.lang.String",
                            valField: "className",
                            entries: [{name: "id", className: "java.lang.Integer"}, {
                                name: "value",
                                className: "java.sql.Timestamp"
                            }]
                        }, {
                            id: "notNullFields",
                            name: "notNullFields",
                            items: ["id"],
                            clsName: "COLLECTION",
                            typeClsName: "java.lang.String",
                            implClsName: "java.util.HashSet"
                        }],
                        arguments: [],
                        clsName: "org.apache.ignite.cache.QueryEntity",
                        id: "qryEntity",
                        src: {
                            id: "d1164cda-f843-4f2d-b0d0-0ce4f0bc2d15",
                            generatePojo: true,
                            caches: ["65743c09-0bcb-48b0-b1c6-88b36b22f2ac"],
                            queryKeyFields: ["id"],
                            queryMetadata: "Configuration",
                            name: "",
                            keyType: "BigDecimal",
                            valueType: "test.ClassName",
                            tableName: "",
                            fields: [{name: "id", className: "Integer", notNull: true}, {
                                name: "value",
                                className: "Timestamp"
                            }],
                            indexes: [],
                            databaseSchema: "schema",
                            databaseTable: "table",
                            keyFields: [{
                                databaseFieldName: "ID",
                                databaseFieldType: "NUMERIC",
                                javaFieldName: "id",
                                javaFieldType: "Integer"
                            }],
                            valueFields: [{
                                databaseFieldName: "VALUE",
                                databaseFieldType: "VARCHAR",
                                javaFieldName: "value",
                                javaFieldType: "Date"
                            }]
                        },
                        dflts: {
                            cacheMode: {clsName: "org.apache.ignite.cache.CacheMode"},
                            partitionLossPolicy: {
                                clsName: "org.apache.ignite.cache.PartitionLossPolicy",
                                value: "IGNORE"
                            },
                            atomicityMode: {clsName: "org.apache.ignite.cache.CacheAtomicityMode"},
                            memoryMode: {
                                clsName: "org.apache.ignite.cache.CacheMemoryMode",
                                value: "ONHEAP_TIERED"
                            },
                            onheapCacheEnabled: false,
                            offHeapMaxMemory: -1,
                            startSize: 1500000,
                            swapEnabled: false,
                            sqlOnheapRowCacheSize: 10240,
                            longQueryWarningTimeout: 3000,
                            snapshotableIndex: false,
                            sqlEscapeAll: false,
                            storeKeepBinary: false,
                            loadPreviousValue: false,
                            cacheStoreFactory: {
                                HiveCacheJdbcPojoStoreFactory: {
                                    batchSize: 512,
                                    maximumWriteAttempts: 2,
                                    parallelLoadCacheMinimumThreshold: 512,
                                    sqlEscapeAll: false,
                                    streamerEnabled: true
                                },
                                CacheJdbcPojoStoreFactory: {
                                    batchSize: 512,
                                    maximumWriteAttempts: 2,
                                    parallelLoadCacheMinimumThreshold: 512,
                                    sqlEscapeAll: false
                                }
                            },
                            storeConcurrentLoadAllThreshold: 5,
                            readThrough: false,
                            writeThrough: false,
                            writeBehindEnabled: false,
                            writeBehindBatchSize: 512,
                            writeBehindFlushSize: 10240,
                            writeBehindFlushFrequency: 5000,
                            writeBehindFlushThreadCount: 1,
                            writeBehindCoalescing: true,
                            maxConcurrentAsyncOperations: 500,
                            defaultLockTimeout: 0,
                            atomicWriteOrderMode: {clsName: "org.apache.ignite.cache.CacheAtomicWriteOrderMode"},
                            writeSynchronizationMode: {
                                clsName: "org.apache.ignite.cache.CacheWriteSynchronizationMode",
                                value: "PRIMARY_SYNC"
                            },
                            rebalanceMode: {
                                clsName: "org.apache.ignite.cache.CacheRebalanceMode",
                                value: "ASYNC"
                            },
                            rebalanceBatchSize: 524288,
                            rebalanceBatchesPrefetchCount: 2,
                            rebalanceOrder: 0,
                            rebalanceDelay: 0,
                            rebalanceTimeout: 10000,
                            rebalanceThrottle: 0,
                            statisticsEnabled: false,
                            managementEnabled: false,
                            nearConfiguration: {nearStartSize: 375000},
                            clientNearConfiguration: {nearStartSize: 375000},
                            evictionPolicy: {batchSize: 1, maxSize: 100000},
                            queryMetadata: "Configuration",
                            queryDetailMetricsSize: 0,
                            queryParallelism: 1,
                            fields: {
                                keyClsName: "java.lang.String",
                                valClsName: "java.lang.String",
                                valField: "className",
                                entries: []
                            },
                            defaultFieldValues: {keyClsName: "java.lang.String", valClsName: "java.lang.Object"},
                            fieldsPrecision: {keyClsName: "java.lang.String", valClsName: "java.lang.Integer"},
                            fieldsScale: {keyClsName: "java.lang.String", valClsName: "java.lang.Integer"},
                            aliases: {
                                keyClsName: "java.lang.String",
                                valClsName: "java.lang.String",
                                keyField: "field",
                                valField: "alias",
                                entries: []
                            },
                            indexes: {
                                indexType: {clsName: "org.apache.ignite.cache.QueryIndexType"},
                                fields: {
                                    keyClsName: "java.lang.String",
                                    valClsName: "java.lang.Boolean",
                                    valField: "direction",
                                    entries: []
                                }
                            },
                            typeField: {databaseFieldType: {clsName: "java.sql.Types"}},
                            memoryPolicyName: "default",
                            diskPageCompression: {clsName: "org.apache.ignite.configuration.DiskPageCompression"},
                            sqlOnheapCacheEnabled: false,
                            sqlOnheapCacheMaxSize: 0,
                            storeByValue: false,
                            encryptionEnabled: false,
                            eventsDisabled: false,
                            maxQueryIteratorsCount: 1024
                        }
                    }],
                    clsName: "COLLECTION",
                    typeClsName: "org.apache.ignite.cache.QueryEntity",
                    implClsName: "java.util.ArrayList"
                }],
                arguments: [],
                clsName: "org.apache.ignite.configuration.CacheConfiguration",
                id: "ccfg",
                src: {
                    id: "65743c09-0bcb-48b0-b1c6-88b36b22f2ac",
                    evictionPolicy: {},
                    cacheMode: "PARTITIONED",
                    atomicityMode: "ATOMIC",
                    readFromBackup: true,
                    copyOnRead: true,
                    cacheStoreFactory: {
                        CacheJdbcBlobStoreFactory: {connectVia: "DataSource"},
                        CacheHibernateBlobStoreFactory: {hibernateProperties: []},
                        kind: "CacheJdbcPojoStoreFactory",
                        CacheJdbcPojoStoreFactory: {dataSourceBean: "ds", dialect: "Generic"}
                    },
                    writeBehindCoalescing: true,
                    nearConfiguration: {},
                    sqlFunctionClasses: [],
                    domains: [{
                        id: "d1164cda-f843-4f2d-b0d0-0ce4f0bc2d15",
                        generatePojo: true,
                        caches: ["65743c09-0bcb-48b0-b1c6-88b36b22f2ac"],
                        queryKeyFields: ["id"],
                        queryMetadata: "Configuration",
                        name: "",
                        keyType: "BigDecimal",
                        valueType: "test.ClassName",
                        tableName: "",
                        fields: [{name: "id", className: "Integer", notNull: true}, {
                            name: "value",
                            className: "Timestamp"
                        }],
                        indexes: [],
                        databaseSchema: "schema",
                        databaseTable: "table",
                        keyFields: [{
                            databaseFieldName: "ID",
                            databaseFieldType: "NUMERIC",
                            javaFieldName: "id",
                            javaFieldType: "Integer"
                        }],
                        valueFields: [{
                            databaseFieldName: "VALUE",
                            databaseFieldType: "VARCHAR",
                            javaFieldName: "value",
                            javaFieldType: "Date"
                        }]
                    }],
                    eagerTtl: true,
                    name: "Cache",
                    readThrough: true,
                    writeThrough: true,
                    plugin: {}
                },
                dflts: {
                    cacheMode: {clsName: "org.apache.ignite.cache.CacheMode"},
                    partitionLossPolicy: {
                        clsName: "org.apache.ignite.cache.PartitionLossPolicy",
                        value: "IGNORE"
                    },
                    atomicityMode: {clsName: "org.apache.ignite.cache.CacheAtomicityMode"},
                    memoryMode: {clsName: "org.apache.ignite.cache.CacheMemoryMode", value: "ONHEAP_TIERED"},
                    onheapCacheEnabled: false,
                    offHeapMaxMemory: -1,
                    startSize: 1500000,
                    swapEnabled: false,
                    sqlOnheapRowCacheSize: 10240,
                    longQueryWarningTimeout: 3000,
                    snapshotableIndex: false,
                    sqlEscapeAll: false,
                    storeKeepBinary: false,
                    loadPreviousValue: false,
                    cacheStoreFactory: {
                        HiveCacheJdbcPojoStoreFactory: {
                            batchSize: 512,
                            maximumWriteAttempts: 2,
                            parallelLoadCacheMinimumThreshold: 512,
                            sqlEscapeAll: false,
                            streamerEnabled: true
                        },
                        CacheJdbcPojoStoreFactory: {
                            batchSize: 512,
                            maximumWriteAttempts: 2,
                            parallelLoadCacheMinimumThreshold: 512,
                            sqlEscapeAll: false
                        }
                    },
                    storeConcurrentLoadAllThreshold: 5,
                    readThrough: false,
                    writeThrough: false,
                    writeBehindEnabled: false,
                    writeBehindBatchSize: 512,
                    writeBehindFlushSize: 10240,
                    writeBehindFlushFrequency: 5000,
                    writeBehindFlushThreadCount: 1,
                    writeBehindCoalescing: true,
                    maxConcurrentAsyncOperations: 500,
                    defaultLockTimeout: 0,
                    atomicWriteOrderMode: {clsName: "org.apache.ignite.cache.CacheAtomicWriteOrderMode"},
                    writeSynchronizationMode: {
                        clsName: "org.apache.ignite.cache.CacheWriteSynchronizationMode",
                        value: "PRIMARY_SYNC"
                    },
                    rebalanceMode: {clsName: "org.apache.ignite.cache.CacheRebalanceMode", value: "ASYNC"},
                    rebalanceBatchSize: 524288,
                    rebalanceBatchesPrefetchCount: 2,
                    rebalanceOrder: 0,
                    rebalanceDelay: 0,
                    rebalanceTimeout: 10000,
                    rebalanceThrottle: 0,
                    statisticsEnabled: false,
                    managementEnabled: false,
                    nearConfiguration: {nearStartSize: 375000},
                    clientNearConfiguration: {nearStartSize: 375000},
                    evictionPolicy: {batchSize: 1, maxSize: 100000},
                    queryMetadata: "Configuration",
                    queryDetailMetricsSize: 0,
                    queryParallelism: 1,
                    fields: {
                        keyClsName: "java.lang.String",
                        valClsName: "java.lang.String",
                        valField: "className",
                        entries: []
                    },
                    defaultFieldValues: {keyClsName: "java.lang.String", valClsName: "java.lang.Object"},
                    fieldsPrecision: {keyClsName: "java.lang.String", valClsName: "java.lang.Integer"},
                    fieldsScale: {keyClsName: "java.lang.String", valClsName: "java.lang.Integer"},
                    aliases: {
                        keyClsName: "java.lang.String",
                        valClsName: "java.lang.String",
                        keyField: "field",
                        valField: "alias",
                        entries: []
                    },
                    indexes: {
                        indexType: {clsName: "org.apache.ignite.cache.QueryIndexType"},
                        fields: {
                            keyClsName: "java.lang.String",
                            valClsName: "java.lang.Boolean",
                            valField: "direction",
                            entries: []
                        }
                    },
                    typeField: {databaseFieldType: {clsName: "java.sql.Types"}},
                    memoryPolicyName: "default",
                    diskPageCompression: {clsName: "org.apache.ignite.configuration.DiskPageCompression"},
                    sqlOnheapCacheEnabled: false,
                    sqlOnheapCacheMaxSize: 0,
                    storeByValue: false,
                    encryptionEnabled: false,
                    eventsDisabled: false,
                    maxQueryIteratorsCount: 1024
                }
            }], typeClsName: "org.apache.ignite.configuration.CacheConfiguration", varArg: true
        }], arguments: [], clsName: "org.apache.ignite.configuration.IgniteConfiguration", id: "cfg", src: {
            id: "ef3cfa11-a9bc-4ffd-8c5d-3726bb78118a",
            activeOnStart: true,
            cacheSanityCheckEnabled: true,
            atomicConfiguration: {},
            cacheKeyConfiguration: [],
            deploymentSpi: {URI: {uriList: [], scanners: []}},
            marshaller: {},
            peerClassLoadingLocalClassPathExclude: [],
            sslContextFactory: {trustManagers: []},
            swapSpaceSpi: {},
            transactionConfiguration: {},
            dataStorageConfiguration: {
                pageSize: null,
                concurrencyLevel: null,
                defaultDataRegionConfiguration: {name: "default"},
                dataRegionConfigurations: []
            },
            memoryConfiguration: {pageSize: null, memoryPolicies: [{name: "default", maxSize: null}]},
            serviceConfigurations: [],
            executorConfiguration: [],
            sqlConnectorConfiguration: {tcpNoDelay: true},
            clientConnectorConfiguration: {
                tcpNoDelay: true,
                jdbcEnabled: true,
                odbcEnabled: true,
                thinClientEnabled: true,
                useIgniteSslContextFactory: true
            },
            discovery: {
                kind: "Multicast",
                Vm: {addresses: ["127.0.0.1:47500..47510"]},
                Multicast: {addresses: ["127.0.0.1:47500..47510"]},
                Jdbc: {initSchema: true},
                Cloud: {regions: [], zones: []}
            },
            binaryConfiguration: {typeConfigurations: [], compactFooter: true},
            communication: {tcpNoDelay: true},
            connector: {noDelay: true},
            collision: {
                kind: "Noop",
                JobStealing: {stealingEnabled: true},
                PriorityQueue: {starvationPreventionEnabled: true}
            },
            failoverSpi: [{kind: "JobStealing"}],
            logger: {Log4j: {mode: "Default"}},
            caches: [{
                id: "65743c09-0bcb-48b0-b1c6-88b36b22f2ac",
                evictionPolicy: {},
                cacheMode: "PARTITIONED",
                atomicityMode: "ATOMIC",
                readFromBackup: true,
                copyOnRead: true,
                cacheStoreFactory: {
                    CacheJdbcBlobStoreFactory: {connectVia: "DataSource"},
                    CacheHibernateBlobStoreFactory: {hibernateProperties: []},
                    kind: "CacheJdbcPojoStoreFactory",
                    CacheJdbcPojoStoreFactory: {dataSourceBean: "ds", dialect: "Generic"}
                },
                writeBehindCoalescing: true,
                nearConfiguration: {},
                sqlFunctionClasses: [],
                domains: [{
                    id: "d1164cda-f843-4f2d-b0d0-0ce4f0bc2d15",
                    generatePojo: true,
                    caches: ["65743c09-0bcb-48b0-b1c6-88b36b22f2ac"],
                    queryKeyFields: ["id"],
                    queryMetadata: "Configuration",
                    name: "",
                    keyType: "BigDecimal",
                    valueType: "test.ClassName",
                    tableName: "",
                    fields: [{name: "id", className: "Integer", notNull: true}, {
                        name: "value",
                        className: "Timestamp"
                    }],
                    indexes: [],
                    databaseSchema: "schema",
                    databaseTable: "table",
                    keyFields: [{
                        databaseFieldName: "ID",
                        databaseFieldType: "NUMERIC",
                        javaFieldName: "id",
                        javaFieldType: "Integer"
                    }],
                    valueFields: [{
                        databaseFieldName: "VALUE",
                        databaseFieldType: "VARCHAR",
                        javaFieldName: "value",
                        javaFieldType: "Date"
                    }]
                }],
                eagerTtl: true,
                name: "Cache",
                readThrough: true,
                writeThrough: true,
                plugin: {}
            }],
            models: ["d1164cda-f843-4f2d-b0d0-0ce4f0bc2d15"],
            checkpointSpi: [],
            loadBalancingSpi: [],
            autoActivationEnabled: true,
            name: "Cluster"
        }, dflts: {
            localHost: "0.0.0.0",
            activeOnStart: true,
            cacheSanityCheckEnabled: true,
            discovery: {
                localPort: 47500,
                localPortRange: 100,
                socketTimeout: 5000,
                ackTimeout: 5000,
                maxAckTimeout: 600000,
                networkTimeout: 5000,
                joinTimeout: 0,
                threadPriority: 10,
                heartbeatFrequency: 2000,
                maxMissedHeartbeats: 1,
                maxMissedClientHeartbeats: 5,
                topHistorySize: 1000,
                reconnectCount: 10,
                statisticsPrintFrequency: 0,
                ipFinderCleanFrequency: 60000,
                forceServerMode: false,
                clientReconnectDisabled: false,
                reconnectDelay: 2000,
                connectionRecoveryTimeout: 10000,
                soLinger: 5,
                Multicast: {
                    multicastGroup: "228.1.2.4",
                    multicastPort: 47400,
                    responseWaitTime: 500,
                    addressRequestAttempts: 2,
                    localAddress: "0.0.0.0"
                },
                Jdbc: {initSchema: false},
                SharedFs: {path: "disco/tcp"},
                ZooKeeper: {
                    basePath: "/services",
                    serviceName: "ignite",
                    allowDuplicateRegistrations: false,
                    ExponentialBackoff: {baseSleepTimeMs: 1000, maxRetries: 10},
                    BoundedExponentialBackoffRetry: {
                        baseSleepTimeMs: 1000,
                        maxSleepTimeMs: 2147483647,
                        maxRetries: 10
                    },
                    UntilElapsed: {maxElapsedTimeMs: 60000, sleepMsBetweenRetries: 1000},
                    RetryNTimes: {n: 10, sleepMsBetweenRetries: 1000},
                    OneTime: {sleepMsBetweenRetry: 1000},
                    Forever: {retryIntervalMs: 1000}
                },
                Kubernetes: {
                    serviceName: "ignite",
                    namespace: "default",
                    masterUrl: "https://kubernetes.default.svc.cluster.local:443",
                    accountToken: "/var/run/secrets/kubernetes.io/serviceaccount/token"
                }
            },
            atomics: {
                atomicSequenceReserveSize: 1000,
                cacheMode: {clsName: "org.apache.ignite.cache.CacheMode", value: "PARTITIONED"}
            },
            binary: {
                compactFooter: true,
                typeConfigurations: {
                    enum: false,
                    enumValues: {keyClsName: "java.lang.String", valClsName: "java.lang.Integer", entries: []}
                }
            },
            collision: {
                kind: null,
                JobStealing: {
                    activeJobsThreshold: 95,
                    waitJobsThreshold: 0,
                    messageExpireTime: 1000,
                    maximumStealingAttempts: 5,
                    stealingEnabled: true,
                    stealingAttributes: {
                        keyClsName: "java.lang.String",
                        valClsName: "java.io.Serializable",
                        items: []
                    }
                },
                PriorityQueue: {
                    priorityAttributeKey: "grid.task.priority",
                    jobPriorityAttributeKey: "grid.job.priority",
                    defaultPriority: 0,
                    starvationIncrement: 1,
                    starvationPreventionEnabled: true
                }
            },
            communication: {
                localPort: 47100,
                localPortRange: 100,
                sharedMemoryPort: 48100,
                directBuffer: false,
                directSendBuffer: false,
                idleConnectionTimeout: 30000,
                connectTimeout: 5000,
                maxConnectTimeout: 600000,
                reconnectCount: 10,
                socketSendBuffer: 32768,
                socketReceiveBuffer: 32768,
                messageQueueLimit: 1024,
                tcpNoDelay: true,
                ackSendThreshold: 16,
                unacknowledgedMessagesBufferSize: 0,
                socketWriteTimeout: 2000,
                selectorSpins: 0,
                connectionsPerNode: 1,
                usePairedConnections: false,
                filterReachableAddresses: false
            },
            networkTimeout: 5000,
            networkSendRetryDelay: 1000,
            networkSendRetryCount: 3,
            discoveryStartupDelay: 60000,
            connector: {
                port: 11211,
                portRange: 100,
                idleTimeout: 7000,
                idleQueryCursorTimeout: 600000,
                idleQueryCursorCheckFrequency: 60000,
                receiveBufferSize: 32768,
                sendBufferSize: 32768,
                sendQueueLimit: 0,
                directBuffer: false,
                noDelay: true,
                sslEnabled: false,
                sslClientAuth: false
            },
            deploymentMode: {clsName: "org.apache.ignite.configuration.DeploymentMode", value: "SHARED"},
            peerClassLoadingEnabled: false,
            peerClassLoadingMissedResourcesCacheSize: 100,
            peerClassLoadingThreadPoolSize: 2,
            failoverSpi: {JobStealing: {maximumFailoverAttempts: 5}, Always: {maximumFailoverAttempts: 5}},
            failureDetectionTimeout: 10000,
            clientFailureDetectionTimeout: 30000,
            logger: {
                Log4j: {level: {clsName: "org.apache.log4j.Level"}},
                Log4j2: {level: {clsName: "org.apache.logging.log4j.Level"}}
            },
            marshalLocalJobs: false,
            marshallerCacheKeepAliveTime: 10000,
            metricsHistorySize: 10000,
            metricsLogFrequency: 60000,
            metricsUpdateFrequency: 2000,
            clockSyncSamples: 8,
            clockSyncFrequency: 120000,
            timeServerPortBase: 31100,
            timeServerPortRange: 100,
            transactionConfiguration: {
                defaultTxConcurrency: {
                    clsName: "org.apache.ignite.transactions.TransactionConcurrency",
                    value: "PESSIMISTIC"
                },
                defaultTxIsolation: {
                    clsName: "org.apache.ignite.transactions.TransactionIsolation",
                    value: "REPEATABLE_READ"
                },
                defaultTxTimeout: 0,
                pessimisticTxLogLinger: 10000,
                useJtaSynchronization: false,
                txTimeoutOnPartitionMapExchange: 0,
                deadlockTimeout: 10000
            },
            attributes: {keyClsName: "java.lang.String", valClsName: "java.lang.String", items: []},
            odbcConfiguration: {
                endpointAddress: "0.0.0.0:10800..10810",
                socketSendBufferSize: 0,
                socketReceiveBufferSize: 0,
                maxOpenCursors: 128
            },
            eventStorage: {Memory: {expireCount: 10000}},
            checkpointSpi: {
                S3: {
                    bucketNameSuffix: "default-bucket",
                    clientConfiguration: {
                        protocol: {clsName: "com.amazonaws.Protocol", value: "HTTPS"},
                        maxConnections: 50,
                        retryPolicy: {
                            retryCondition: {clsName: "com.amazonaws.retry.PredefinedRetryPolicies"},
                            backoffStrategy: {clsName: "com.amazonaws.retry.PredefinedRetryPolicies"},
                            maxErrorRetry: {clsName: "com.amazonaws.retry.PredefinedRetryPolicies"},
                            honorMaxErrorRetryInClientConfig: false
                        },
                        maxErrorRetry: -1,
                        socketTimeout: 50000,
                        connectionTimeout: 50000,
                        requestTimeout: 0,
                        socketSendBufferSizeHints: 0,
                        connectionTTL: -1,
                        connectionMaxIdleMillis: 60000,
                        responseMetadataCacheSize: 50,
                        useReaper: true,
                        useGzip: false,
                        preemptiveBasicProxyAuth: false,
                        useTcpKeepAlive: false,
                        cacheResponseMetadata: true,
                        clientExecutionTimeout: 0,
                        socketSendBufferSizeHint: 0,
                        socketReceiveBufferSizeHint: 0,
                        useExpectContinue: true,
                        useThrottleRetries: true
                    }
                },
                JDBC: {
                    checkpointTableName: "CHECKPOINTS",
                    keyFieldName: "NAME",
                    keyFieldType: "VARCHAR",
                    valueFieldName: "VALUE",
                    valueFieldType: "BLOB",
                    expireDateFieldName: "EXPIRE_DATE",
                    expireDateFieldType: "DATETIME",
                    numberOfRetries: 2
                }
            },
            loadBalancingSpi: {
                RoundRobin: {perTask: false},
                Adaptive: {
                    loadProbe: {
                        Job: {useAverage: true},
                        CPU: {useAverage: true, useProcessors: true, processorCoefficient: 1},
                        ProcessingTime: {useAverage: true}
                    }
                },
                WeightedRandom: {nodeWeight: 10, useWeights: false}
            },
            memoryConfiguration: {
                systemCacheInitialSize: 41943040,
                systemCacheMaxSize: 104857600,
                pageSize: 2048,
                defaultMemoryPolicyName: "default",
                memoryPolicies: {
                    name: "default",
                    initialSize: 268435456,
                    pageEvictionMode: {
                        clsName: "org.apache.ignite.configuration.DataPageEvictionMode",
                        value: "DISABLED"
                    },
                    evictionThreshold: 0.9,
                    emptyPagesPoolSize: 100,
                    metricsEnabled: false,
                    subIntervals: 5,
                    rateTimeInterval: 60000
                }
            },
            dataStorageConfiguration: {
                systemCacheInitialSize: 41943040,
                systemCacheMaxSize: 104857600,
                pageSize: 4096,
                storagePath: "db",
                dataRegionConfigurations: {
                    name: "default",
                    initialSize: 268435456,
                    pageEvictionMode: {
                        clsName: "org.apache.ignite.configuration.DataPageEvictionMode",
                        value: "DISABLED"
                    },
                    evictionThreshold: 0.9,
                    emptyPagesPoolSize: 100,
                    metricsEnabled: false,
                    metricsSubIntervalCount: 5,
                    metricsRateTimeInterval: 60000,
                    checkpointPageBufferSize: 0,
                    lazyMemoryAllocation: true
                },
                metricsEnabled: false,
                alwaysWriteFullPages: false,
                checkpointFrequency: 180000,
                checkpointPageBufferSize: 268435456,
                checkpointThreads: 4,
                checkpointWriteOrder: {
                    clsName: "org.apache.ignite.configuration.CheckpointWriteOrder",
                    value: "SEQUENTIAL"
                },
                walMode: {clsName: "org.apache.ignite.configuration.WALMode", value: "DEFAULT"},
                walPath: "db/wal",
                walArchivePath: "db/wal/archive",
                walSegments: 10,
                walSegmentSize: 67108864,
                walHistorySize: 20,
                walFlushFrequency: 2000,
                walFsyncDelayNanos: 1000,
                walRecordIteratorBufferSize: 67108864,
                lockWaitTime: 10000,
                walThreadLocalBufferSize: 131072,
                metricsSubIntervalCount: 5,
                metricsRateTimeInterval: 60000,
                maxWalArchiveSize: 1073741824,
                walCompactionLevel: 1,
                walPageCompression: {
                    clsName: "org.apache.ignite.configuration.DiskPageCompression",
                    value: "DISABLED"
                }
            },
            utilityCacheKeepAliveTime: 60000,
            serviceConfigurations: {maxPerNodeCount: 0, totalCount: 0},
            longQueryWarningTimeout: 3000,
            persistenceStoreConfiguration: {
                metricsEnabled: false,
                alwaysWriteFullPages: false,
                checkpointingFrequency: 180000,
                checkpointingPageBufferSize: 268435456,
                checkpointingThreads: 1,
                walSegments: 10,
                walSegmentSize: 67108864,
                walHistorySize: 20,
                walFlushFrequency: 2000,
                walFsyncDelayNanos: 1000,
                walRecordIteratorBufferSize: 67108864,
                lockWaitTime: 10000,
                rateTimeInterval: 60000,
                tlbSize: 131072,
                subIntervals: 5,
                walMode: {clsName: "org.apache.ignite.configuration.WALMode", value: "DEFAULT"},
                walAutoArchiveAfterInactivity: -1
            },
            sqlConnectorConfiguration: {
                port: 10800,
                portRange: 100,
                socketSendBufferSize: 0,
                socketReceiveBufferSize: 0,
                tcpNoDelay: true,
                maxOpenCursorsPerConnection: 128
            },
            clientConnectorConfiguration: {
                port: 10800,
                portRange: 100,
                socketSendBufferSize: 0,
                socketReceiveBufferSize: 0,
                tcpNoDelay: true,
                maxOpenCursorsPerConnection: 128,
                idleTimeout: 0,
                jdbcEnabled: true,
                odbcEnabled: true,
                thinClientEnabled: true,
                sslEnabled: false,
                useIgniteSslContextFactory: true,
                sslClientAuth: false,
                handshakeTimeout: 10000
            },
            encryptionSpi: {Keystore: {keySize: 256, masterKeyName: "ignite.master.key"}},
            failureHandler: {ignoredFailureTypes: {clsName: "org.apache.ignite.failure.FailureType"}},
            localEventListeners: {
                keyClsName: "org.apache.ignite.lang.IgnitePredicate",
                keyClsGenericType: "org.apache.ignite.events.Event",
                isKeyClsGenericTypeExtended: true,
                valClsName: "int[]",
                valClsNameShow: "EVENTS",
                keyField: "className",
                valField: "eventTypes"
            },
            authenticationEnabled: false,
            sqlQueryHistorySize: 1000,
            allSegmentationResolversPassRequired: true,
            networkCompressionLevel: 1,
            autoActivationEnabled: true,
            rebalanceBatchSize: 524288,
            rebalanceBatchesPrefetchCount: 2,
            rebalanceTimeout: 10000,
            rebalanceThrottle: 0
        }
    };
});
