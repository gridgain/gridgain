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

import template from './ui-ace-sharp.pug';
import controller from './ui-ace-sharp.controller';

/**
 * @param {import('app/modules/configuration/generator/SharpTransformer.service').default} generator
 */
export default function directive(generator) {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} $el
     * @param {ng.IAttributes} attrs
     * @param {[typeof controller, any?, ng.IFormController?, ng.INgModelController?]} controllers
     */
    const link = (scope, $el, attrs, controllers) => {
        const [ctrl, igniteUiAceTabs, formCtrl, ngModelCtrl] = controllers;
        if (formCtrl && ngModelCtrl)
            formCtrl.$removeControl(ngModelCtrl);

        if (igniteUiAceTabs && igniteUiAceTabs.onLoad) {
            scope.onLoad = (editor) => {
                igniteUiAceTabs.onLoad(editor);

                scope.$watch('master', () => editor.attractAttention = false);
            };
        }

        if (igniteUiAceTabs && igniteUiAceTabs.onChange)
            scope.onChange = igniteUiAceTabs.onChange;

        const render = (data) => {
            delete ctrl.data;

            if (!data)
                return;

            return ctrl.generator(scope.master);
        };

        // Setup generator.
        if (scope.generator) {
            const method = scope.generator;

            switch (method) {
                case 'clusterCaches':
                    ctrl.generator = (cluster) => {
                        const caches = _.reduce(scope.detail, (acc, cache) => {
                            if (_.includes(cluster.caches, cache.value))
                                acc.push(cache.cache);

                            return acc;
                        }, []);

                        return generator.clusterCaches(cluster, caches, null, true).asString();
                    };

                    break;

                case 'igfss':
                    ctrl.generator = (cluster) => {
                        const igfss = _.reduce(scope.detail, (acc, igfs) => {
                            if (_.includes(cluster.igfss, igfs.value))
                                acc.push(igfs.igfs);

                            return acc;
                        }, []);

                        return generator.igfss(igfss, 'cfg').asString();
                    };

                    break;

                case 'cacheStore':
                    ctrl.generator = (cache) => {
                        const domains = _.reduce(scope.detail, (acc, domain) => {
                            if (_.includes(cache.domains, domain.value))
                                acc.push(domain.meta);

                            return acc;
                        }, []);

                        return generator.cacheStore(cache, domains).asString();
                    };

                    break;

                default:
                    ctrl.generator = (data) => generator[method](data).asString();
            }
        }

        if (!_.isUndefined(attrs.clusterCfg)) {
            scope.$watch('cfg', (cfg) => {
                if (!_.isUndefined(cfg))
                    return;

                scope.cfg = {};
            });

            scope.$watch('cfg', (data) => ctrl.data = render(data), true);
        }

        const noDeepWatch = !(typeof attrs.noDeepWatch !== 'undefined');

        // Setup watchers.
        scope.$watch('master', (data) => ctrl.data = render(data), noDeepWatch);
    };

    return {
        priority: 1,
        restrict: 'E',
        scope: {
            master: '=',
            detail: '=',
            generator: '@',
            cfg: '=?clusterCfg'
        },
        bindToController: {
            data: '=?ngModel'
        },
        link,
        template,
        controller,
        controllerAs: 'ctrl',
        require: ['igniteUiAceSharp', '?^igniteUiAceTabs', '?^form', '?ngModel']
    };
}

directive.$inject = ['IgniteSharpTransformer'];
