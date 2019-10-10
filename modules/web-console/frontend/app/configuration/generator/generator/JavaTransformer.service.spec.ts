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
});
