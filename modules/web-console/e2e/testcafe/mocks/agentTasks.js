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

import _ from 'lodash';

export const taskResult = (result) => ({
    response: {result},
    error: null,
    sessionToken: null,
    successStatus: 0
});

/**
 * Generate features set with ids from 0 to 95.
 *
 * @param {Array<number>} excluded Array with features to exclude.
 * @return {string} Base64 coded string of enabled features.
 */
export const generateIgniteFeatures = (excluded = []) => {
    const res = Buffer.alloc(12);

    for (let i = 0; i < res.length * 8; i ++) {
        if (_.indexOf(excluded, i) < 0) {
            const idx = Math.floor(i / (res.BYTES_PER_ELEMENT * 8));
            const shift = i % (res.BYTES_PER_ELEMENT * 8);

            res[idx] = res[idx] ^ (1 << shift);
        }
    }

    return res.toString('base64');
};

export const DFLT_FAILURE_RESPONSE = {message: 'Expected error'};

export const cacheNamesCollectorTask = (caches) => (ws) => {
    ws.on('node:visor', (e) => {
        if (e.params.taskId === 'cacheNamesCollectorTask')
            return taskResult(caches);
    });
};

export const simeplFakeSQLQuery = (nid, response) => (ws) => {
    ws.on('node:visor', (e) => {
        switch (e.params.taskId) {
            case 'cacheNodesTaskX2':
                return taskResult([nid]);

            case 'querySqlX2': {
                if (e.params.nids === nid) {
                    return taskResult({
                        error: null,
                        result: {
                            columns: null,
                            duration: 0,
                            hasMore: false,
                            queryId: 'query-1',
                            responseNodeId: nid,
                            rows: null
                        }
                    });
                }

                break;
            }

            case 'queryFetchFirstPage': {
                if (e.params.nids === nid)
                    return taskResult(response);

                break;
            }
        }
    });
};

export const WC_SCHEDULING_NOT_AVAILABLE = 24;

const CLUSTER_1 = {
    id: '70831a7c-2b5e-4c11-8c08-5888911d5962',
    name: 'Cluster 1',
    clusterVersion: '8.8.0-SNAPSHOT',
    active: true,
    secured: false,
    supportedFeatures: generateIgniteFeatures([WC_SCHEDULING_NOT_AVAILABLE]),
    nodes: {
        '143048f1-b5b8-47d6-9239-fed76222efe3': {
            address: '10.0.75.1',
            client: false
        }
    }
};

const CLUSTER_2 = {
    id: '70831a7c-2b5e-4c11-8c08-5888911d5963',
    name: 'Cluster 2',
    clusterVersion: '8.8.0-SNAPSHOT',
    active: true,
    secured: false,
    supportedFeatures: generateIgniteFeatures([WC_SCHEDULING_NOT_AVAILABLE]),
    nodes: {
        '143048f1-b5b8-47d6-9239-fed76222efe4': {
            address: '10.0.75.1',
            client: false
        }
    }
};

export const AGENT_ONLY_NO_CLUSTER = {
    hasAgent: true,
    hasDemo: true,
    clusters: []
};

export const FAKE_CLUSTERS = {
    hasAgent: true,
    hasDemo: true,
    clusters: [CLUSTER_1, CLUSTER_2]
};

export const AGENT_DISCONNECTED_GRID = {
    hasAgent: false,
    hasDemo: false,
    clusters: []
};

export const INACTIVE_CLUSTER = {
    hasAgent: true,
    hasDemo: true,
    clusters: [Object.assign({ ...CLUSTER_1 }, { active: false })]
};

export const SIMPLE_QUERY_RESPONSE = {
    error: null,
    result: {
        rows: [
            [1, 'Ed'],
            [2, 'Ann'],
            [3, 'Emma']
        ],
        hasMore: false,
        duration: 0,
        columns: [{
            schemaName: 'PUBLIC',
            typeName: 'PERSON',
            fieldName: 'ID',
            fieldTypeName: 'java.lang.Integer'
        }, {
            schemaName: 'PUBLIC',
            typeName: 'PERSON',
            fieldName: 'NAME',
            fieldTypeName: 'java.lang.String'
        }],
        queryId: 'VISOR_SQL_QUERY-42b1b723-874e-48eb-a760-b6357fc71c7f',
        responseNodeId: '0daf9042-21e6-4dd3-8f8e-a3187246abe4'
    }
};

export const SIMPLE_FAILED_QUERY_RESPONSE = {
    error: {
        message: 'Outer error message',
        stackTrace: [
            'Outer error trace 1',
            'Outer error trace 2'
        ],
        cause: {
            message: 'Inner error message',
            stackTrace: [
                'Inner error trace 1',
                'Inner error trace 2'
            ],
            cause: {
                message: 'Cause without stacktrace'
            }
        }
    },
    result: null
};

export const FAKE_CACHES = {
    caches: {
        Cache1: 'a',
        Cache2: 'b'
    },
    groups: []
};

export const agentStat = (clusters) => (ws) => {
    ws.emit('agent:status', clusters);
};

/**
 * Return error responce on request with specified event type.
 *
 * @param {string} eventType
 * @returns {(ws: import('./WebSocketHook').WebSocket) => void}
 */
export const errorResponseForEventType = (eventType) => (ws) => {
    ws.errorOn(eventType, () => DFLT_FAILURE_RESPONSE);
};

export const TEST_JDBC_IMPORT_DATA = {
    drivers: [{
        'jdbcDriverJar': 'test-driver.jar',
        'jdbcDriverClass': 'test.dvired.cls',
        'jdbcDriverImplVersion': 'test.version'
    }],
    schemas: {
        catalog: 'TESTDB',
        schemas: ['PUBLIC']
    },
    tables: [{
        action: 1,
        cacheOrTemplate: -1,
        columns: [{
            name: 'ID',
            type: 4,
            unsigned: false,
            key: true,
            nullable: false
        }, {
            name: 'NAME',
            type: 12,
            unsigned: false,
            key: false,
            nullable: true
        }],
        edit: false,
        generatedCacheName: 'TestCache',
        id: 0,
        indexes: [],
        label: 'PUBLIC.TEST',
        schema: 'PUBLIC',
        table: 'TEST'
    }]
};

export const FULL_LIST_COLUMN_TYPES = [{
    type: -7,
    expectedType: 'boolean'
}, {
    type: -6,
    unsigned: false,
    expectedType: 'byte'
}, {
    type: -6,
    unsigned: true,
    expectedType: 'short'
}, {
    type: 5,
    unsigned: false,
    expectedType: 'short'
}, {
    type: 5,
    unsigned: true,
    expectedType: 'int'
}, {
    type: 4,
    unsigned: false,
    expectedType: 'int'
}, {
    type: 4,
    unsigned: true,
    expectedType: 'long'
}, {
    type: -5,
    expectedType: 'long'
}, {
    type: 6,
    expectedType: 'float'
}, {
    type: 7,
    expectedType: 'double'
}, {
    type: 8,
    expectedType: 'double'
}, {
    type: 2,
    expectedType: 'BigDecimal'
}, {
    type: 3,
    expectedType: 'BigDecimal'
}, {
    type: 1,
    expectedType: 'String'
}, {
    type: 12,
    expectedType: 'String'
}, {
    type: -1,
    expectedType: 'String'
}, {
    type: 91,
    expectedType: 'Date'
}, {
    type: 92,
    expectedType: 'Time'
}, {
    type: 93,
    expectedType: 'Timestamp'
}, {
    type: -2,
    expectedType: 'byte[]'
}, {
    type: -3,
    expectedType: 'byte[]'
}, {
    type: -4,
    expectedType: 'byte[]'
}, {
    type: 0,
    expectedType: 'Object'
}, {
    type: 1111,
    expectedType: 'Object'
}, {
    type: 2000,
    expectedType: 'Object'
}, {
    type: 2001,
    expectedType: 'Object'
}, {
    type: 2002,
    expectedType: 'Object'
}, {
    type: 2003,
    expectedType: 'Object'
}, {
    type: 2004,
    expectedType: 'Object'
}, {
    type: 2005,
    expectedType: 'String'
}, {
    type: 2006,
    expectedType: 'Object'
}, {
    type: 70,
    expectedType: 'Object'
}, {
    type: 16,
    expectedType: 'boolean'
}, {
    type: -8,
    expectedType: 'Object'
}, {
    type: -15,
    expectedType: 'String'
}, {
    type: -9,
    expectedType: 'String'
}, {
    type: -16,
    expectedType: 'String'
}, {
    type: 2011,
    expectedType: 'String'
}, {
    type: 2009,
    expectedType: 'Object'
}];

export const schemaImportRequest = (requestData) => (ws) => {
    ws.on('schemaImport:drivers', (e) => {
        return requestData.drivers;
    });

    ws.on('schemaImport:schemas', (e) => {
        return requestData.schemas;
    });

    ws.on('schemaImport:metadata', (e) => {
        return requestData.tables;
    });
};
