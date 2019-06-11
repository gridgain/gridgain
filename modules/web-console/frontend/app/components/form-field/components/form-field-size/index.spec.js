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

import 'mocha';
import {assert} from 'chai';
import angular from 'angular';
import componentModule from '../../index';

suite('form-field-size', () => {
    /** @type {ng.IScope} */
    let $scope;
    /** @type {ng.ICompileService} */
    let $compile;
    angular.module('test', [componentModule.name]);

    const click = (el) => el[0].querySelector('.panel-collapsible__status-icon').click();

    setup(() => {
        angular.module('test', [componentModule.name]);
        angular.mock.module('test');
        angular.mock.inject((_$rootScope_, _$compile_) => {
            $compile = _$compile_;
            $scope = _$rootScope_.$new();
        });
    });

    test('Switch editor measure', async() => {
        $scope.model = 1;
        const el = angular.element(`
            <form-field-size
                ng-model='model'
                id='period'
                name='period'
                label='period:'
                size-type='time'
                size-scale-label='sec'
                min='0'
            />
        `);
        $compile(el)($scope);
        $scope.$digest();
        const ctrl = $scope.$$childHead.$ctrl;
        ctrl.sizeScale = {label: 'hour', value: 60 * 60};
        $scope.$digest();
        assert.equal($scope.model, 60*60, 'Model value is recalculated on measure switch');
    });
});
