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
import {IInputErrorNotifier} from 'app/types';

type IgniteFormFieldScope < T > = ng.IScope & ({$input: T} | {[name: string]: T});

export class IgniteFormField<T> implements IInputErrorNotifier {
    static animName = 'ignite-form-field__error-blink';
    static eventName = 'webkitAnimationEnd oAnimationEnd msAnimationEnd animationend';
    static $inject = ['$element', '$scope'];
    onAnimEnd: () => any | null;

    constructor(private $element: JQLite, private $scope: IgniteFormFieldScope<T>) {}

    $postLink() {
        this.onAnimEnd = () => this.$element.removeClass(IgniteFormField.animName);
        this.$element.on(IgniteFormField.eventName, this.onAnimEnd);
    }

    $onDestroy() {
        this.$element.off(IgniteFormField.eventName, this.onAnimEnd);
        this.$element = this.onAnimEnd = null;
    }

    notifyAboutError() {
        if (!this.$element)
            return;

        if (this.isTooltipValidation())
            this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseenter');
        else
            this.$element.addClass(IgniteFormField.animName);
    }

    hideError() {
        if (!this.$element)
            return;

        if (this.isTooltipValidation())
            this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseleave');
    }

    isTooltipValidation(): boolean {
        return !this.$element.parents('.theme--ignite-errors-horizontal').length;
    }

    /**
     * Exposes control in $scope
     */
    exposeControl(control: ng.INgModelController, name = '$input') {
        this.$scope[name] = control;
        this.$scope.$on('$destroy', () => this.$scope[name] = null);
    }
}

export function directive<T>(): ng.IDirective<IgniteFormFieldScope<T>> {
    return {
        restrict: 'C',
        controller: IgniteFormField,
        scope: true
    };
}
