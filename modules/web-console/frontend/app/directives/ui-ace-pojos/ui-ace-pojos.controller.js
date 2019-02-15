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
import {nonNil} from 'app/utils/lodashMixins';

export default function controller($scope, JavaTypes, generator) {
    const ctrl = this;

    this.$onInit = () => {
        // Watchers definition.
        // Watcher clean instance data if instance to cluster caches was change
        const cleanPojos = () => {
            delete ctrl.class;
            delete ctrl.pojos;
            delete ctrl.classes;
        };

        // Watcher update pojos when changes caches and checkers useConstructor and includeKeyFields
        const updatePojos = () => {
            delete ctrl.pojos;

            if (_.isNil(ctrl.cluster) || _.isEmpty(ctrl.cluster.caches))
                return;

            ctrl.pojos = generator.pojos(ctrl.cluster.caches, ctrl.useConstructor, ctrl.includeKeyFields);
        };

        // Watcher update classes after
        const updateClasses = (value) => {
            delete ctrl.classes;

            if (!value)
                return;

            const classes = ctrl.classes = [];

            _.forEach(ctrl.pojos, (pojo) => {
                if (nonNil(pojo.keyClass))
                    classes.push(pojo.keyType);

                classes.push(pojo.valueType);
            });
        };

        // Update pojos class.
        const updateClass = (value) => {
            if (_.isEmpty(value))
                return;

            const pojo = value[0];

            ctrl.class = ctrl.class || (pojo.keyClass ? pojo.keyType : pojo.valueType);
        };

        // Update pojos data.
        const updatePojosData = (value) => {
            if (_.isNil(value))
                return;

            _.forEach(ctrl.pojos, (pojo) => {
                if (pojo.keyType === ctrl.class) {
                    ctrl.data = pojo.keyClass;

                    return false;
                }

                if (pojo.valueType === ctrl.class) {
                    ctrl.data = pojo.valueClass;

                    return false;
                }
            });
        };

        // Setup watchers. Watchers order is important.
        $scope.$watch('ctrl.cluster.caches', cleanPojos);
        $scope.$watch('ctrl.cluster.caches', updatePojos);
        $scope.$watch('ctrl.cluster.caches', updateClasses);
        $scope.$watch('ctrl.useConstructor', updatePojos);
        $scope.$watch('ctrl.includeKeyFields', updatePojos);
        $scope.$watch('ctrl.pojos', updateClass);
        $scope.$watch('ctrl.pojos', updatePojosData);
        $scope.$watch('ctrl.class', updatePojosData);
    };
}

controller.$inject = ['$scope', 'JavaTypes', 'JavaTransformer'];
