/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

/// <reference path="./types/uirouter.d.ts" />

declare namespace ig {
    type menu < T > = Array<{value: T, label: string}>;

    namespace config {
        namespace formFieldSize {
            interface ISizeTypeOption {
                label: string,
                value: number
            }
            type ISizeType = Array<ISizeTypeOption>;
            interface ISizeTypes {
                [name: string]: ISizeType
            }
        }

        namespace cluster {
            export type DiscoveryKinds = 'Vm'
                | 'Multicast'
                | 'S3'
                | 'Cloud'
                | 'GoogleStorage'
                | 'Jdbc'
                | 'SharedFs'
                | 'ZooKeeper'
                | 'Kubernetes';

            export type LoadBalancingKinds = 'RoundRobin'
                | 'Adaptive'
                | 'WeightedRandom'
                | 'Custom';

            export type FailoverSPIs = 'JobStealing' | 'Never' | 'Always' | 'Custom';

            export interface ShortCluster {
                _id: string,
                name: string,
                discovery: DiscoveryKinds,
                caches: number,
                models: number,
                igfs: number
            }
        }

        namespace cache {
            type CacheModes = 'PARTITIONED' | 'REPLICATED' | 'LOCAL';
            type AtomicityModes = 'ATOMIC' | 'TRANSACTIONAL';
            export interface ShortCache {
                _id: string,
                cacheMode: CacheModes,
                atomicityMode: AtomicityModes,
                backups: number
            }
        }

        namespace model {
            type QueryMetadataTypes = 'Annotations' | 'Configuration';
            type DomainModelKinds = 'query' | 'store' | 'both';
            export interface KeyField {
                databaseFieldName: string,
                databaseFieldType: string,
                javaFieldName: string,
                javaFieldType: string
            }
            export interface ValueField {
                databaseFieldName: string,
                databaseFieldType: string,
                javaFieldName: string,
                javaFieldType: string
            }
            interface Field {
                name: string,
                className: string
            }
            interface Alias {
                field: string,
                alias: string
            }
            type IndexTypes = 'SORTED' | 'FULLTEXT' | 'GEOSPATIAL';
            export interface IndexField {
                _id: string,
                name?: string,
                direction?: boolean
            }
            export interface Index {
                _id: string,
                name: string,
                indexType: IndexTypes,
                fields: Array<IndexField>
            }

            export interface DomainModel {
                _id: string,
                space?: string,
                clusters?: Array<string>,
                caches?: Array<string>,
                queryMetadata?: QueryMetadataTypes,
                kind?: DomainModelKinds,
                tableName?: string,
                keyFieldName?: string,
                valueFieldName?: string,
                databaseSchema?: string,
                databaseTable?: string,
                keyType?: string,
                valueType?: string,
                keyFields?: Array<KeyField>,
                valueFields?: Array<ValueField>,
                queryKeyFields?: Array<string>,
                fields?: Array<Field>,
                aliases?: Array<Alias>,
                indexes?: Array<Index>,
                generatePojo?: boolean
            }

            export interface ShortDomainModel {
                _id: string,
                keyType: string,
                valueType: string,
                hasIndex: boolean
            }
        }

        namespace igfs {
            type DefaultModes = 'PRIMARY' | 'PROXY' | 'DUAL_SYNC' | 'DUAL_ASYNC';
            export interface ShortIGFS {
                _id: string,
                name: string,
                defaultMode: DefaultModes,
                affinnityGroupSize: number
            }
        }
    }
}

export as namespace ig
export = ig
