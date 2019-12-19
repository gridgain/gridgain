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

import {isPersistenceEnabled} from './configuration-checker';
import {assert} from 'chai';

suite('Test of configuration checker', () => {
    test('Check persistence in configuration', () => {
        const NO_PERSISTENCE_CFG = {
            dataStorageConfiguration: {
                defaultDataRegionConfiguration: {
                    persistenceEnabled: false
                },
                dataRegionConfigurations: [{
                    persistenceEnabled: false
                }]
            }
        };

        const DFLT_PERSISTENCE_CFG = {
            dataStorageConfiguration: {
                defaultDataRegionConfiguration: {
                    persistenceEnabled: true
                }
            }
        };

        const REGION_PERSISTENCE_CFG = {
            dataStorageConfiguration: {
                dataRegionConfigurations: [{
                    persistenceEnabled: true
                }]
            }
        };

        assert.isFalse(isPersistenceEnabled(NO_PERSISTENCE_CFG), 'Wrong detection of persistence on the cluster.');
        assert.isTrue(isPersistenceEnabled(DFLT_PERSISTENCE_CFG), 'Persistence on the cluster is not detected.');
        assert.isTrue(isPersistenceEnabled(REGION_PERSISTENCE_CFG));
    });
});
