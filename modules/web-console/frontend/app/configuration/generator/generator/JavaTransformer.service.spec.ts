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
import cloneDeep from 'lodash/cloneDeep';
import find from 'lodash/find';
import forEach from 'lodash/forEach';

import * as testData from './JavaTransformer.service.data.spec';

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
        assert.equal(_generate(false), testData.CACHE_LOAD_WO_PERSISTENCE_CONTENT);
        assert.equal(_generate(true), testData.CACHE_LOAD_WITH_PERSISTENCE_CONTENT);
    });

    test('Should generate valid list of imports', () => {
        const configuration = cloneDeep(testData.TEST_CONFIGURATION);

        _addCacheConfiguration(configuration, testData.TEST_CACHE);

        const imports = IgniteJavaTransformer._prepareImports(
            IgniteJavaTransformer.collectConfigurationImports(configuration)
        );

        assert.equal(testData.EXPECTED_IMPORTS.length, imports.length);

        forEach(testData.EXPECTED_IMPORTS,
            (expectedImport) => assert.equal(true, imports.indexOf(expectedImport) >= 0)
        );
    });

    test('Should generate valid list of static imports', () => {
        const configuration = cloneDeep(testData.TEST_CONFIGURATION);

        const imports = IgniteJavaTransformer._prepareImports(
            IgniteJavaTransformer.collectStaticImports(configuration)
        );

        assert.equal(testData.EXPECTED_STATIC_IMPORTS.length, imports.length);

        forEach(testData.EXPECTED_STATIC_IMPORTS,
            (expectedImport) => assert.equal(true, imports.indexOf(expectedImport) >= 0)
        );
    });

    test('Should generate list of imports for big configuration without exceptions', () => {
        const configuration = cloneDeep(testData.TEST_CONFIGURATION);

        _addCacheConfiguration(configuration, testData.TEST_CACHE, 4000);

        IgniteJavaTransformer.collectConfigurationImports(configuration);
    }).timeout(0);
});
