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
import {assert} from 'chai';
import {outdent} from 'outdent/lib';
import cloneDeep from 'lodash/cloneDeep';
import filter from 'lodash/filter';
import find from 'lodash/find';
import forEach from 'lodash/forEach';
import startsWith from 'lodash/startsWith';
import includes from 'lodash/includes';

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

    const TEST_CACHE = {
        properties: [{
            clsName: 'org.apache.ignite.cache.CacheMode',
            name: 'cacheMode',
            value: 'PARTITIONED'
        }, {
            clsName: 'org.apache.ignite.cache.CacheAtomicityMode',
            name: 'atomicityMode',
            value: 'ATOMIC'
        }, {clsName: 'int', name: 'copyOnRead', value: true}, {
            clsName: 'BEAN', name: 'cacheStoreFactory', value: {
                properties: [{
                    clsName: 'DATA_SOURCE',
                    id: 'ds',
                    name: 'dataSourceBean',
                    value: {
                        properties: [{
                            clsName: 'PROPERTY',
                            name: 'URL',
                            value: 'ds.jdbc.url',
                            hint: 'jdbc:oracle:thin:@[host]:[port]:[database]'
                        }, {
                            clsName: 'PROPERTY',
                            name: 'user',
                            value: 'ds.jdbc.username',
                            hint: 'YOUR_USER_NAME'
                        }, {
                            clsName: 'PROPERTY',
                            name: 'password',
                            value: 'ds.jdbc.password',
                            hint: 'YOUR_PASSWORD'
                        }],
                        arguments: [],
                        clsName: 'oracle.jdbc.pool.OracleDataSource',
                        id: 'ds',
                        src: {},
                        dflts: {}
                    }
                }, {
                    clsName: 'BEAN',
                    name: 'dialect',
                    value: {
                        properties: [],
                        arguments: [],
                        clsName: 'org.apache.ignite.cache.store.jdbc.dialect.OracleDialect'
                    }
                }, {
                    clsName: 'ARRAY', id: 'types', name: 'types', items: [{
                        properties: [{
                            clsName: 'java.lang.String',
                            name: 'cacheName',
                            value: 'Cache'
                        }, {
                            clsName: 'java.lang.Class',
                            name: 'keyType',
                            value: 'BigDecimal'
                        }, {
                            clsName: 'java.lang.String',
                            name: 'valueType',
                            value: 'test.ClassName'
                        }, {
                            clsName: 'java.lang.String',
                            name: 'databaseSchema',
                            value: 'schema'
                        }, {
                            clsName: 'java.lang.String',
                            name: 'databaseTable',
                            value: 'table'
                        }, {
                            clsName: 'ARRAY',
                            id: 'keyFields',
                            name: 'keyFields',
                            items: [{
                                properties: [],
                                arguments: [{
                                    clsName: 'java.sql.Types',
                                    constant: true,
                                    value: 'NUMERIC'
                                }, {
                                    clsName: 'java.lang.String',
                                    name: null,
                                    value: 'ID'
                                }, {
                                    clsName: 'java.lang.Class',
                                    name: null,
                                    value: 'Integer'
                                }, {clsName: 'java.lang.String', name: null, value: 'id'}],
                                clsName: 'org.apache.ignite.cache.store.jdbc.JdbcTypeField',
                                id: 'typeField',
                                src: {
                                    databaseFieldName: 'ID',
                                    databaseFieldType: 'NUMERIC',
                                    javaFieldName: 'id',
                                    javaFieldType: 'Integer'
                                },
                                dflts: {}
                            }],
                            typeClsName: 'org.apache.ignite.cache.store.jdbc.JdbcTypeField',
                            varArg: true
                        }, {
                            clsName: 'ARRAY',
                            id: 'valueFields',
                            name: 'valueFields',
                            items: [{
                                properties: [],
                                arguments: [{
                                    clsName: 'java.sql.Types',
                                    constant: true,
                                    value: 'VARCHAR'
                                }, {
                                    clsName: 'java.lang.String',
                                    name: null,
                                    value: 'VALUE'
                                }, {
                                    clsName: 'java.lang.Class',
                                    name: null,
                                    value: 'Date'
                                }, {clsName: 'java.lang.String', name: null, value: 'value'}],
                                clsName: 'org.apache.ignite.cache.store.jdbc.JdbcTypeField',
                                id: 'typeField',
                                src: {
                                    databaseFieldName: 'VALUE',
                                    databaseFieldType: 'VARCHAR',
                                    javaFieldName: 'value',
                                    javaFieldType: 'Date'
                                },
                                dflts: {}
                            }],
                            typeClsName: 'org.apache.ignite.cache.store.jdbc.JdbcTypeField',
                            varArg: true
                        }],
                        arguments: [],
                        clsName: 'org.apache.ignite.cache.store.jdbc.JdbcType',
                        id: 'type',
                        src: {
                            id: 'd1164cda-f843-4f2d-b0d0-0ce4f0bc2d15',
                            generatePojo: true,
                            caches: ['65743c09-0bcb-48b0-b1c6-88b36b22f2ac'],
                            queryKeyFields: ['id'],
                            queryMetadata: 'Configuration',
                            name: '',
                            keyType: 'BigDecimal',
                            valueType: 'test.ClassName',
                            tableName: '',
                            fields: [{
                                name: 'id',
                                className: 'Integer',
                                notNull: true
                            }, {name: 'value', className: 'Timestamp'}],
                            indexes: [{
                                id: 'cea9d17d-ba54-45a0-be9a-fa0a30676cc0',
                                name: 'queryIndex',
                                indexType: 'SORTED',
                                fields: [{
                                    id: '33261e7e-c590-4475-98e6-485cd0678f03',
                                    direction: true,
                                    name: 'id'
                                }]
                            }],
                            databaseSchema: 'schema',
                            databaseTable: 'table',
                            keyFields: [{
                                databaseFieldName: 'ID',
                                databaseFieldType: 'NUMERIC',
                                javaFieldName: 'id',
                                javaFieldType: 'Integer'
                            }],
                            valueFields: [{
                                databaseFieldName: 'VALUE',
                                databaseFieldType: 'VARCHAR',
                                javaFieldName: 'value',
                                javaFieldType: 'Date'
                            }],
                            cacheName: 'Cache'
                        },
                        dflts: {}
                    }], typeClsName: 'org.apache.ignite.cache.store.jdbc.JdbcType', varArg: true
                }],
                arguments: [],
                clsName: 'org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory',
                id: 'cacheStoreFactory',
                src: {dataSourceBean: 'ds', dialect: 'Oracle'},
                dflts: {}
            }
        }, {clsName: 'boolean', name: 'readThrough', value: true}, {
            clsName: 'boolean',
            name: 'writeThrough',
            value: true
        }, {clsName: 'boolean', name: 'eagerTtl', value: true}, {
            id: 'qryEntities',
            name: 'queryEntities',
            items: [{
                properties: [{
                    clsName: 'java.lang.String',
                    name: 'keyType',
                    value: 'java.math.BigDecimal'
                }, {
                    clsName: 'java.lang.String',
                    name: 'valueType',
                    value: 'test.ClassName'
                }, {
                    id: 'keyFields',
                    name: 'keyFields',
                    items: ['id'],
                    clsName: 'COLLECTION',
                    typeClsName: 'java.lang.String',
                    implClsName: 'java.util.HashSet'
                }, {
                    clsName: 'MAP',
                    id: 'fields',
                    name: 'fields',
                    ordered: true,
                    keyClsName: 'java.lang.String',
                    keyField: 'name',
                    valClsName: 'java.lang.String',
                    valField: 'className',
                    entries: [{name: 'id', className: 'java.lang.Integer'}, {
                        name: 'value',
                        className: 'java.sql.Timestamp'
                    }]
                }, {
                    id: 'notNullFields',
                    name: 'notNullFields',
                    items: ['id'],
                    clsName: 'COLLECTION',
                    typeClsName: 'java.lang.String',
                    implClsName: 'java.util.HashSet'
                }, {
                    id: 'indexes',
                    name: 'indexes',
                    items: [{
                        properties: [{
                            clsName: 'java.lang.String',
                            name: 'name',
                            value: 'queryIndex'
                        }, {
                            clsName: 'org.apache.ignite.cache.QueryIndexType',
                            name: 'indexType',
                            value: 'SORTED'
                        }, {
                            clsName: 'MAP',
                            id: 'indFlds',
                            name: 'fields',
                            ordered: true,
                            keyClsName: 'java.lang.String',
                            keyField: 'name',
                            valClsName: 'java.lang.Boolean',
                            valField: 'direction',
                            entries: [{
                                id: '33261e7e-c590-4475-98e6-485cd0678f03',
                                direction: true,
                                name: 'id'
                            }]
                        }],
                        arguments: [],
                        clsName: 'org.apache.ignite.cache.QueryIndex',
                        id: 'index',
                        src: {
                            id: 'cea9d17d-ba54-45a0-be9a-fa0a30676cc0',
                            name: 'queryIndex',
                            indexType: 'SORTED',
                            fields: [{
                                id: '33261e7e-c590-4475-98e6-485cd0678f03',
                                direction: true,
                                name: 'id'
                            }]
                        },
                        dflts: {}
                    }],
                    clsName: 'COLLECTION',
                    typeClsName: 'org.apache.ignite.cache.QueryIndex',
                    implClsName: 'java.util.ArrayList'
                }],
                arguments: [],
                clsName: 'org.apache.ignite.cache.QueryEntity',
                id: 'qryEntity',
                src: {
                    id: 'd1164cda-f843-4f2d-b0d0-0ce4f0bc2d15',
                    generatePojo: true,
                    caches: ['65743c09-0bcb-48b0-b1c6-88b36b22f2ac'],
                    queryKeyFields: ['id'],
                    queryMetadata: 'Configuration',
                    name: '',
                    keyType: 'BigDecimal',
                    valueType: 'test.ClassName',
                    tableName: '',
                    fields: [{name: 'id', className: 'Integer', notNull: true}, {
                        name: 'value',
                        className: 'Timestamp'
                    }],
                    indexes: [{
                        id: 'cea9d17d-ba54-45a0-be9a-fa0a30676cc0',
                        name: 'queryIndex',
                        indexType: 'SORTED',
                        fields: [{
                            id: '33261e7e-c590-4475-98e6-485cd0678f03',
                            direction: true,
                            name: 'id'
                        }]
                    }],
                    databaseSchema: 'schema',
                    databaseTable: 'table',
                    keyFields: [{
                        databaseFieldName: 'ID',
                        databaseFieldType: 'NUMERIC',
                        javaFieldName: 'id',
                        javaFieldType: 'Integer'
                    }],
                    valueFields: [{
                        databaseFieldName: 'VALUE',
                        databaseFieldType: 'VARCHAR',
                        javaFieldName: 'value',
                        javaFieldType: 'Date'
                    }]
                },
                dflts: {}
            }],
            clsName: 'COLLECTION',
            typeClsName: 'org.apache.ignite.cache.QueryEntity',
            implClsName: 'java.util.ArrayList'
        }],
        arguments: [],
        clsName: 'org.apache.ignite.configuration.CacheConfiguration',
        id: 'ccfg',
        src: {
            id: '65743c09-0bcb-48b0-b1c6-88b36b22f2ac',
            evictionPolicy: {},
            cacheMode: 'PARTITIONED',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            cacheStoreFactory: {
                CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'},
                CacheHibernateBlobStoreFactory: {hibernateProperties: []},
                kind: 'CacheJdbcPojoStoreFactory',
                CacheJdbcPojoStoreFactory: {dataSourceBean: 'ds', dialect: 'Oracle'}
            },
            writeBehindCoalescing: true,
            nearConfiguration: {enabled: false},
            sqlFunctionClasses: [],
            domains: [{
                id: 'd1164cda-f843-4f2d-b0d0-0ce4f0bc2d15',
                generatePojo: true,
                caches: ['65743c09-0bcb-48b0-b1c6-88b36b22f2ac'],
                queryKeyFields: ['id'],
                queryMetadata: 'Configuration',
                name: '',
                keyType: 'BigDecimal',
                valueType: 'test.ClassName',
                tableName: '',
                fields: [{name: 'id', className: 'Integer', notNull: true}, {
                    name: 'value',
                    className: 'Timestamp'
                }],
                indexes: [{
                    id: 'cea9d17d-ba54-45a0-be9a-fa0a30676cc0',
                    name: 'queryIndex',
                    indexType: 'SORTED',
                    fields: [{id: '33261e7e-c590-4475-98e6-485cd0678f03', direction: true, name: 'id'}]
                }],
                databaseSchema: 'schema',
                databaseTable: 'table',
                keyFields: [{
                    databaseFieldName: 'ID',
                    databaseFieldType: 'NUMERIC',
                    javaFieldName: 'id',
                    javaFieldType: 'Integer'
                }],
                valueFields: [{
                    databaseFieldName: 'VALUE',
                    databaseFieldType: 'VARCHAR',
                    javaFieldName: 'value',
                    javaFieldType: 'Date'
                }]
            }],
            eagerTtl: true,
            name: 'Cache',
            readThrough: true,
            writeThrough: true,
            clientNearConfiguration: {enabled: true},
            plugin: {}
        },
        dflts: {}
    };

    const TEST_CONFIGURATION = {
        properties: [{clsName: 'boolean', name: 'clientMode', value: true}, {
            clsName: 'java.lang.String',
            name: 'igniteInstanceName',
            value: 'Cluster'
        }, {
            clsName: 'BEAN', name: 'discoverySpi', value: {
                properties: [{
                    clsName: 'BEAN',
                    name: 'ipFinder',
                    value: {
                        properties: [{
                            id: 'addrs',
                            name: 'addresses',
                            items: ['127.0.0.1:47500..47510'],
                            clsName: 'COLLECTION',
                            typeClsName: 'java.lang.String',
                            implClsName: 'java.util.ArrayList'
                        }],
                        arguments: [],
                        clsName: 'org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder',
                        id: 'ipFinder',
                        src: {addresses: ['127.0.0.1:47500..47510']},
                        dflts: {}
                    }
                }],
                arguments: [],
                clsName: 'org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi',
                id: 'discovery',
                src: {
                    kind: 'Multicast',
                    Vm: {addresses: ['127.0.0.1:47500..47510']},
                    Multicast: {addresses: ['127.0.0.1:47500..47510']},
                    Jdbc: {initSchema: true},
                    Cloud: {regions: [], zones: []}
                },
                dflts: {}
            }
        }, {
            clsName: 'ARRAY',
            id: 'failoverSpi',
            name: 'failoverSpi',
            items: [{
                properties: [],
                arguments: [],
                clsName: 'org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi',
                id: 'failoverSpi',
                dflts: {}
            }],
            typeClsName: 'org.apache.ignite.spi.failover.FailoverSpi'
        }, {
            clsName: 'ARRAY', id: 'ccfgs', name: 'cacheConfiguration', items: [], typeClsName: 'org.apache.ignite.configuration.CacheConfiguration', varArg: true
        }, {
            clsName: 'ARRAY', id: 'pluginConfigurations', name: 'pluginConfigurations', items: [{
                properties: [{
                    clsName: 'BEAN',
                    name: 'securityCredentialsProvider',
                    value: {
                        properties: [],
                        arguments: [{
                            clsName: 'BEAN',
                            id: 'cred',
                            value: {
                                properties: [],
                                arguments: [{
                                    clsName: 'PROPERTY',
                                    value: 'security.jaas.user.name',
                                    hint: 'YOUR_SECURITY_JAAS_USER_NAME'
                                }, {
                                    clsName: 'PROPERTY',
                                    value: 'security.jaas.user.password',
                                    hint: 'YOUR_SECURITY_JAAS_USER_PASSWORD'
                                }],
                                clsName: 'org.apache.ignite.plugin.security.SecurityCredentials',
                                id: 'permsForJaasUser',
                                src: {},
                                dflts: {}
                            }
                        }],
                        clsName: 'org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider',
                        id: 'credsProvider',
                        src: {},
                        dflts: {}
                    }
                }, {
                    clsName: 'BEAN', name: 'authenticator', value: {
                        properties: [{
                            clsName: 'BEAN', name: 'defaultPermissionSet', value: {
                                properties: [{
                                    clsName: 'boolean',
                                    name: 'defaultAllowAll',
                                    value: false
                                }, {
                                    id: 'systemPermissions',
                                    name: 'systemPermissions',
                                    items: ['ADMIN_VIEW', 'ADMIN_OPS'],
                                    clsName: 'ENUM_COLLECTION',
                                    typeClsName: 'org.apache.ignite.plugin.security.SecurityPermission',
                                    implClsName: 'java.util.ArrayList'
                                }, {
                                    clsName: 'MAP',
                                    id: 'cachePermissions',
                                    name: 'cachePermissions',
                                    ordered: false,
                                    keyClsName: 'java.lang.String',
                                    keyField: 'name',
                                    valClsName: 'java.util.Collection',
                                    valField: 'perms',
                                    entries: [{name: 'a', perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE']}],
                                    valClsGenericType: 'org.apache.ignite.plugin.security.SecurityPermission'
                                }],
                                arguments: [],
                                clsName: 'org.apache.ignite.plugin.security.SecurityBasicPermissionSet',
                                id: 'dfltPermSet',
                                src: {
                                    systemPerms: ['ADMIN_VIEW', 'ADMIN_OPS'],
                                    cachePerms: [{
                                        id: 'ec6674d7-165d-4776-9b94-26b922153f46',
                                        mode: 'Pattern',
                                        caches: [],
                                        perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                        ptrn: 'a'
                                    }],
                                    taskPerms: [],
                                    servicePerms: [],
                                    userName: '',
                                    allowAll: false
                                },
                                dflts: {}
                            }
                        }, {
                            clsName: 'BEAN', name: 'permissionsProvider', value: {
                                properties: [],
                                arguments: [{
                                    clsName: 'MAP',
                                    id: 'perms',
                                    keyClsName: 'java.lang.String',
                                    keyField: 'name',
                                    valClsName: 'org.apache.ignite.plugin.security.SecurityPermissionSet',
                                    valField: 'value',
                                    entries: [{
                                        name: 'user', value: {
                                            properties: [{
                                                clsName: 'boolean',
                                                name: 'defaultAllowAll',
                                                value: false
                                            }, {
                                                clsName: 'MAP',
                                                id: 'cachePermissions',
                                                name: 'cachePermissions',
                                                ordered: false,
                                                keyClsName: 'java.lang.String',
                                                keyField: 'name',
                                                valClsName: 'java.util.Collection',
                                                valField: 'perms',
                                                entries: [{
                                                    name: 'cache',
                                                    perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE']
                                                }],
                                                valClsGenericType: 'org.apache.ignite.plugin.security.SecurityPermission'
                                            }],
                                            arguments: [],
                                            clsName: 'org.apache.ignite.plugin.security.SecurityBasicPermissionSet',
                                            id: '_userPermSet',
                                            src: {
                                                systemPerms: [],
                                                cachePerms: [{
                                                    id: '1eda9fde-562a-43aa-974f-30cae4ca4b3f',
                                                    mode: 'Pattern',
                                                    caches: [],
                                                    perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                                    ptrn: 'cache'
                                                }],
                                                taskPerms: [],
                                                servicePerms: [],
                                                userName: 'user',
                                                allowAll: false
                                            },
                                            dflts: {}
                                        }
                                    }]
                                }],
                                clsName: 'org.gridgain.grid.security.jaas.JaasBasicPermissionsProvider',
                                id: 'jaasPermsProvider',
                                src: {
                                    securityEnabled: true,
                                    authenticationMode: 'JAAS',
                                    permissions: [{
                                        systemPerms: [],
                                        cachePerms: [{
                                            id: '1eda9fde-562a-43aa-974f-30cae4ca4b3f',
                                            mode: 'Pattern',
                                            caches: [],
                                            perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                            ptrn: 'cache'
                                        }],
                                        taskPerms: [],
                                        servicePerms: [],
                                        userName: 'user',
                                        allowAll: false
                                    }],
                                    defaultPermissionSet: {
                                        systemPerms: ['ADMIN_VIEW', 'ADMIN_OPS'],
                                        cachePerms: [{
                                            id: 'ec6674d7-165d-4776-9b94-26b922153f46',
                                            mode: 'Pattern',
                                            caches: [],
                                            perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                            ptrn: 'a'
                                        }],
                                        taskPerms: [],
                                        servicePerms: [],
                                        userName: '',
                                        allowAll: false
                                    },
                                    baseCredentialsUser: 'user',
                                    authzIdentity: 'identity',
                                    permissionsProvider: 'BASIC',
                                    jsonPermissions: '{}'
                                },
                                dflts: {}
                            }
                        }],
                        arguments: [],
                        clsName: 'org.gridgain.grid.security.jaas.JaasAuthenticator',
                        id: 'authenticator',
                        src: {
                            securityEnabled: true,
                            authenticationMode: 'JAAS',
                            permissions: [{
                                systemPerms: [],
                                cachePerms: [{
                                    id: '1eda9fde-562a-43aa-974f-30cae4ca4b3f',
                                    mode: 'Pattern',
                                    caches: [],
                                    perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                    ptrn: 'cache'
                                }],
                                taskPerms: [],
                                servicePerms: [],
                                userName: 'user',
                                allowAll: false
                            }],
                            defaultPermissionSet: {
                                systemPerms: ['ADMIN_VIEW', 'ADMIN_OPS'],
                                cachePerms: [{
                                    id: 'ec6674d7-165d-4776-9b94-26b922153f46',
                                    mode: 'Pattern',
                                    caches: [],
                                    perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                    ptrn: 'a'
                                }],
                                taskPerms: [],
                                servicePerms: [],
                                userName: '',
                                allowAll: false
                            },
                            baseCredentialsUser: 'user',
                            authzIdentity: 'identity',
                            permissionsProvider: 'BASIC',
                            jsonPermissions: '{}'
                        },
                        dflts: {}
                    }
                }],
                arguments: [],
                clsName: 'org.gridgain.grid.configuration.GridGainConfiguration',
                id: 'plugin',
                src: {
                    id: 'ef3cfa11-a9bc-4ffd-8c5d-3726bb78118a',
                    gridgainEnabled: true,
                    rollingUpdatesEnabled: false,
                    clusters: ['ef3cfa11-a9bc-4ffd-8c5d-3726bb78118a'],
                    security: {
                        securityEnabled: true,
                        authenticationMode: 'JAAS',
                        permissions: [{
                            systemPerms: [],
                            cachePerms: [{
                                id: '1eda9fde-562a-43aa-974f-30cae4ca4b3f',
                                mode: 'Pattern',
                                caches: [],
                                perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                ptrn: 'cache'
                            }],
                            taskPerms: [],
                            servicePerms: [],
                            userName: 'user',
                            allowAll: false
                        }],
                        defaultPermissionSet: {
                            systemPerms: ['ADMIN_VIEW', 'ADMIN_OPS'],
                            cachePerms: [{
                                id: 'ec6674d7-165d-4776-9b94-26b922153f46',
                                mode: 'Pattern',
                                caches: [],
                                perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                                ptrn: 'a'
                            }],
                            taskPerms: [],
                            servicePerms: [],
                            userName: '',
                            allowAll: false
                        },
                        baseCredentialsUser: 'user',
                        authzIdentity: 'identity',
                        permissionsProvider: 'BASIC',
                        jsonPermissions: '{}'
                    },
                    snapshotConfiguration: {enabled: false},
                    dr: {
                        dataCenterId: '1',
                        sender: {
                            senderEnabled: false,
                            store: {fs: {checksumEnabled: true}},
                            cacheConfiguration: [],
                            connectionConfiguration: [{
                                id: '20f7de25-46bc-45e8-ac8e-16d750cd1835',
                                dataCenterId: '2',
                                awaitAcknowledge: true,
                                receiverAddresses: ['127.0.0.1:49000']
                            }],
                            useIgniteSslContextFactory: true
                        },
                        receiver: {
                            receiverEnabled: false,
                            tcpNodelay: true,
                            directBuffer: true,
                            useIgniteSslContextFactory: true,
                            cacheConfiguration: []
                        }
                    }
                },
                dflts: {}
            }], typeClsName: 'org.gridgain.grid.configuration.GridGainConfiguration', varArg: true
        }], arguments: [], clsName: 'org.apache.ignite.configuration.IgniteConfiguration', id: 'cfg', src: {
            id: 'ef3cfa11-a9bc-4ffd-8c5d-3726bb78118a',
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
                defaultDataRegionConfiguration: {name: 'default'},
                dataRegionConfigurations: []
            },
            memoryConfiguration: {pageSize: null, memoryPolicies: [{name: 'default', maxSize: null}]},
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
                kind: 'Multicast',
                Vm: {addresses: ['127.0.0.1:47500..47510']},
                Multicast: {addresses: ['127.0.0.1:47500..47510']},
                Jdbc: {initSchema: true},
                Cloud: {regions: [], zones: []}
            },
            binaryConfiguration: {typeConfigurations: [], compactFooter: true},
            communication: {tcpNoDelay: true},
            connector: {noDelay: true},
            collision: {
                kind: 'Noop',
                JobStealing: {stealingEnabled: true},
                PriorityQueue: {starvationPreventionEnabled: true}
            },
            failoverSpi: [{kind: 'JobStealing'}],
            logger: {Log4j: {mode: 'Default'}},
            caches: [{
                id: '65743c09-0bcb-48b0-b1c6-88b36b22f2ac',
                evictionPolicy: {},
                cacheMode: 'PARTITIONED',
                atomicityMode: 'ATOMIC',
                readFromBackup: true,
                copyOnRead: true,
                cacheStoreFactory: {
                    CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'},
                    CacheHibernateBlobStoreFactory: {hibernateProperties: []},
                    kind: 'CacheJdbcPojoStoreFactory',
                    CacheJdbcPojoStoreFactory: {dataSourceBean: 'ds', dialect: 'Oracle'}
                },
                writeBehindCoalescing: true,
                nearConfiguration: {enabled: false},
                sqlFunctionClasses: [],
                domains: [{
                    id: 'd1164cda-f843-4f2d-b0d0-0ce4f0bc2d15',
                    generatePojo: true,
                    caches: ['65743c09-0bcb-48b0-b1c6-88b36b22f2ac'],
                    queryKeyFields: ['id'],
                    queryMetadata: 'Configuration',
                    name: '',
                    keyType: 'BigDecimal',
                    valueType: 'test.ClassName',
                    tableName: '',
                    fields: [{name: 'id', className: 'Integer', notNull: true}, {
                        name: 'value',
                        className: 'Timestamp'
                    }],
                    indexes: [{
                        id: 'cea9d17d-ba54-45a0-be9a-fa0a30676cc0',
                        name: 'queryIndex',
                        indexType: 'SORTED',
                        fields: [{id: '33261e7e-c590-4475-98e6-485cd0678f03', direction: true, name: 'id'}]
                    }],
                    databaseSchema: 'schema',
                    databaseTable: 'table',
                    keyFields: [{
                        databaseFieldName: 'ID',
                        databaseFieldType: 'NUMERIC',
                        javaFieldName: 'id',
                        javaFieldType: 'Integer'
                    }],
                    valueFields: [{
                        databaseFieldName: 'VALUE',
                        databaseFieldType: 'VARCHAR',
                        javaFieldName: 'value',
                        javaFieldType: 'Date'
                    }]
                }],
                eagerTtl: true,
                name: 'Cache',
                readThrough: true,
                writeThrough: true,
                clientNearConfiguration: {enabled: true},
                plugin: {}
            }],
            models: ['d1164cda-f843-4f2d-b0d0-0ce4f0bc2d15'],
            checkpointSpi: [],
            loadBalancingSpi: [],
            autoActivationEnabled: true,
            name: 'Cluster',
            plugin: {
                id: 'ef3cfa11-a9bc-4ffd-8c5d-3726bb78118a',
                gridgainEnabled: true,
                rollingUpdatesEnabled: false,
                clusters: ['ef3cfa11-a9bc-4ffd-8c5d-3726bb78118a'],
                security: {
                    securityEnabled: true,
                    authenticationMode: 'JAAS',
                    permissions: [{
                        systemPerms: [],
                        cachePerms: [{
                            id: '1eda9fde-562a-43aa-974f-30cae4ca4b3f',
                            mode: 'Pattern',
                            caches: [],
                            perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                            ptrn: 'cache'
                        }],
                        taskPerms: [],
                        servicePerms: [],
                        userName: 'user',
                        allowAll: false
                    }],
                    defaultPermissionSet: {
                        systemPerms: ['ADMIN_VIEW', 'ADMIN_OPS'],
                        cachePerms: [{
                            id: 'ec6674d7-165d-4776-9b94-26b922153f46',
                            mode: 'Pattern',
                            caches: [],
                            perms: ['CACHE_READ', 'CACHE_PUT', 'CACHE_REMOVE'],
                            ptrn: 'a'
                        }],
                        taskPerms: [],
                        servicePerms: [],
                        userName: '',
                        allowAll: false
                    },
                    baseCredentialsUser: 'user',
                    authzIdentity: 'identity',
                    permissionsProvider: 'BASIC',
                    jsonPermissions: '{}'
                },
                snapshotConfiguration: {enabled: false},
                dr: {
                    dataCenterId: '1',
                    sender: {
                        senderEnabled: false,
                        store: {fs: {checksumEnabled: true}},
                        cacheConfiguration: [],
                        connectionConfiguration: [{
                            id: '20f7de25-46bc-45e8-ac8e-16d750cd1835',
                            dataCenterId: '2',
                            awaitAcknowledge: true,
                            receiverAddresses: ['127.0.0.1:49000']
                        }],
                        useIgniteSslContextFactory: true
                    },
                    receiver: {
                        receiverEnabled: false,
                        tcpNodelay: true,
                        directBuffer: true,
                        useIgniteSslContextFactory: true,
                        cacheConfiguration: []
                    }
                }
            }
        }, dflts: {}
    };

    const _addCacheConfiguration = (cfg, cacheCfg, count = 1) => {
        const caches = find(cfg.properties, {name: 'cacheConfiguration'});

        if (caches) {
            for (let i = 0; i < count; i++) {
                const nameProp = {
                    clsName: 'java.lang.String',
                    name: 'name',
                    value: `Cache${i}`
                };

                const cache = cloneDeep(cacheCfg);

                cache.properties.push(nameProp);
                caches.items.push(cache);
            }
        }
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
        const EXPECTED_IMPORTS = [
            'java.io.InputStream',
            'java.math.BigDecimal',
            'java.sql.Date',
            'java.sql.SQLException',
            'java.sql.Types',
            'java.util.ArrayList',
            'java.util.Arrays',
            'java.util.Collection',
            'java.util.HashMap',
            'java.util.HashSet',
            'java.util.LinkedHashMap',
            'java.util.Properties',
            'javax.cache.configuration.Factory',
            'javax.sql.DataSource',
            'oracle.jdbc.pool.OracleDataSource',
            'org.apache.ignite.cache.CacheAtomicityMode',
            'org.apache.ignite.cache.CacheMode',
            'org.apache.ignite.cache.QueryEntity',
            'org.apache.ignite.cache.QueryIndex',
            'org.apache.ignite.cache.QueryIndexType',
            'org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory',
            'org.apache.ignite.cache.store.jdbc.JdbcType',
            'org.apache.ignite.cache.store.jdbc.JdbcTypeField',
            'org.apache.ignite.cache.store.jdbc.dialect.OracleDialect',
            'org.apache.ignite.configuration.CacheConfiguration',
            'org.apache.ignite.configuration.IgniteConfiguration',
            'org.apache.ignite.plugin.security.SecurityBasicPermissionSet',
            'org.apache.ignite.plugin.security.SecurityCredentials',
            'org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider',
            'org.apache.ignite.plugin.security.SecurityPermission',
            'org.apache.ignite.plugin.security.SecurityPermissionSet',
            'org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi',
            'org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder',
            'org.apache.ignite.spi.failover.FailoverSpi',
            'org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi',
            'org.gridgain.grid.configuration.GridGainConfiguration',
            'org.gridgain.grid.security.jaas.JaasAuthenticator',
            'org.gridgain.grid.security.jaas.JaasBasicPermissionsProvider'
        ];

        const configuration = cloneDeep(TEST_CONFIGURATION);

        _addCacheConfiguration(configuration, TEST_CACHE);

        const imports = filter(
            [...IgniteJavaTransformer.collectConfigurationImports(configuration).values()],
            (cls) => !startsWith(cls, 'java.lang.') && includes(cls, '.')
        );

        assert.equal(EXPECTED_IMPORTS.length, imports.length);

        forEach(EXPECTED_IMPORTS, (expectedImport) => assert.equal(true, imports.indexOf(expectedImport) >= 0));
    });

    test('Should generate list of imports for big configuration without exceptions', () => {
        const configuration = cloneDeep(TEST_CONFIGURATION);

        _addCacheConfiguration(configuration, TEST_CACHE, 1000);

        IgniteJavaTransformer.collectConfigurationImports(configuration);
    }).timeout(0);
});
