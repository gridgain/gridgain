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

import _ from 'lodash';

export default class ArtifactVersionChecker {
    /**
     * Compare two numbers.
     *
     * @param a {Number} First number to compare
     * @param b {Number} Second number to compare.
     * @return {Number} 1 when a is greater then b, -1 when b is greater then a, 0 when a and b is equal.
     */
    static _numberComparator(a, b) {
        return a > b ? 1 : a < b ? -1 : 0;
    }

    /**
     * Compare to version.
     *
     * @param {Object} a first compared version.
     * @param {Object} b second compared version.
     * @returns {Number} 1 if a > b, 0 if versions equals, -1 if a < b
     */
    static _compare(a, b) {
        for (let i = 0; i < a.length && i < b.length; i++) {
            const res = this._numberComparator(a[i], b[i]);

            if (res !== 0)
                return res;
        }

        return 0;
    }

    /**
     * Tries to parse JDBC driver version.
     *
     * @param {String} ver - String representation of version.
     * @returns {Number[]} - Array of version parts.
     */
    static _parse(ver) {
        return _.map(ver.split(/[.-]/), (v) => {
            return v.startsWith('jre') ? parseInt(v.substring(3), 10) : parseInt(v, 10);
        });
    }

    /**
     * Stay only latest versions of the same dependencies.
     *
     * @param deps Array of dependencies.
     */
    static latestVersions(deps) {
        return _.map(_.values(_.groupBy(_.uniqWith(deps, _.isEqual), (dep) => dep.groupId + dep.artifactId)), (arr) => {
            if (arr.length > 1) {
                try {
                    return _.reduce(arr, (resDep, dep) => {
                        if (this._compare(this._parse(dep.version), this._parse(resDep.version)) > 0)
                            return dep;

                        return resDep;
                    });
                }
                catch (err) {
                    return _.last(_.sortBy(arr, 'version'));
                }
            }

            return arr[0];
        });
    }
}
