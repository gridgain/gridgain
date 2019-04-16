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

import {merge} from 'rxjs';
import {tap, pluck, distinctUntilChanged} from 'rxjs/operators';

import {WellKnownOperationStatus} from 'app/types';
import {IgniteChartController} from '../../controller';

const BLANK_STATUS = new Set([WellKnownOperationStatus.ERROR, WellKnownOperationStatus.WAITING]);

export default class IgniteChartNoDataCtrl implements ng.IOnChanges, ng.IOnDestroy {
    static $inject = ['AgentManager'];

    constructor(private AgentManager) {}

    igniteChart: IgniteChartController;

    handleClusterInactive: boolean;

    connectionState$ = this.AgentManager.connectionSbj.pipe(
        pluck('state'),
        distinctUntilChanged(),
        tap((state) => {
            if (state === 'AGENT_DISCONNECTED')
                this.destroyChart();
        })
    );

    cluster$ = this.AgentManager.connectionSbj.pipe(
        pluck('cluster'),
        distinctUntilChanged(),
        tap((cluster) => {
            if (!cluster && !this.AgentManager.isDemoMode()) {
                this.destroyChart();
                return;
            }

            if (!!cluster && cluster.active === false && this.handleClusterInactive)
                this.destroyChart();

        })
    );

    subsribers$ = merge(
        this.connectionState$,
        this.cluster$
    ).subscribe();

    $onChanges(changes) {
        if (changes.resultDataStatus && BLANK_STATUS.has(changes.resultDataStatus.currentValue))
            this.destroyChart();
    }

    $onDestroy() {
        this.subsribers$.unsubscribe();
    }

    destroyChart() {
        if (this.igniteChart && this.igniteChart.chart) {
            this.igniteChart.chart.destroy();
            this.igniteChart.config = null;
            this.igniteChart.chart = null;
        }
    }
}
