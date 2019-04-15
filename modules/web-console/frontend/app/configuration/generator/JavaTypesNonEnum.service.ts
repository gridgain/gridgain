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

import uniq from 'lodash/uniq';
import map from 'lodash/map';
import reduce from 'lodash/reduce';
import isObject from 'lodash/isObject';
import merge from 'lodash/merge';
import includes from 'lodash/includes';

export class JavaTypesNonEnum {
    static $inject = ['IgniteClusterDefaults', 'IgniteCacheDefaults', 'IgniteIGFSDefaults', 'JavaTypes'];

    enumClasses: any;
    shortEnumClasses: any;

    constructor(clusterDflts, cacheDflts, igfsDflts, JavaTypes) {
        this.enumClasses = uniq(this._enumClassesAcc(merge(clusterDflts, cacheDflts, igfsDflts), []));
        this.shortEnumClasses = map(this.enumClasses, (cls) => JavaTypes.shortClassName(cls));
    }

    /**
     * Check if class name is non enum class in Ignite configuration.
     *
     * @param clsName
     * @return {boolean}
     */
    nonEnum(clsName: string): boolean {
        return !includes(this.shortEnumClasses, clsName) && !includes(this.enumClasses, clsName);
    }

    /**
     * Collects recursive enum classes.
     *
     * @param root Root object.
     * @param classes Collected classes.
     */
    private _enumClassesAcc(root, classes): Array<string> {
        return reduce(root, (acc, val, key) => {
            if (key === 'clsName')
                acc.push(val);
            else if (isObject(val))
                this._enumClassesAcc(val, acc);

            return acc;
        }, classes);
    }
}
