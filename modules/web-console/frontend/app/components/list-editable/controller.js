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

/** @type {ng.IComponentController} */
export default class {
    /** @type {ng.INgModelController} */
    ngModel;

    static $inject = ['$animate', '$element', '$transclude', '$timeout'];

    /**
     * @param {ng.animate.IAnimateService} $animate
     * @param {JQLite} $element
     * @param {ng.ITranscludeFunction} $transclude
     * @param {ng.ITimeoutService} $timeout
     */
    constructor($animate, $element, $transclude, $timeout) {
        $animate.enabled($element, false);
        this.$transclude = $transclude;
        this.$element = $element;
        this.$timeout = $timeout;
        this.hasItemView = $transclude.isSlotFilled('itemView');

        this._cache = {};
    }

    $index(item, $index) {
        if (item._id)
            return item._id;

        return $index;
    }

    $onDestroy() {
        this.$element = null;
    }

    $onInit() {
        this.ngModel.$isEmpty = (value) => {
            return !Array.isArray(value) || !value.length;
        };
        this.ngModel.editListItem = (item) => {
            this.$timeout(() => {
                this.startEditView(this.ngModel.$viewValue.indexOf(item));
                // For some reason required validator does not re-run after adding an item,
                // the $validate call fixes the issue.
                this.ngModel.$validate();
            });
        };
        this.ngModel.editListIndex = (index) => {
            this.$timeout(() => {
                this.startEditView(index);
                // For some reason required validator does not re-run after adding an item,
                // the $validate call fixes the issue.
                this.ngModel.$validate();
            });
        };
    }

    save(data, idx) {
        this.ngModel.$setViewValue(this.ngModel.$viewValue.map((v, i) => i === idx ? _.cloneDeep(data) : v));
    }

    revert(idx) {
        delete this._cache[idx];
    }

    remove(idx) {
        this.ngModel.$setViewValue(this.ngModel.$viewValue.filter((v, i) => i !== idx));
    }

    isEditView(idx) {
        return this._cache.hasOwnProperty(idx);
    }

    getEditView(idx) {
        return this._cache[idx];
    }

    startEditView(idx) {
        this._cache[idx] = _.cloneDeep(this.ngModel.$viewValue[idx]);
    }

    stopEditView(data, idx, form) {
        // By default list-editable saves only valid values, but if you specify {allowInvalid: true}
        // ng-model-option, then it will always save. Be careful and pay extra attention to validation
        // when doing so, it's an easy way to miss invalid values this way.

        // Don't close if form is invalid and allowInvalid is turned off (which is default value)
        if (!form.$valid && !this.ngModel.$options.getOption('allowInvalid'))
            return;

        delete this._cache[idx];

        this.save(data, idx);
    }
}
