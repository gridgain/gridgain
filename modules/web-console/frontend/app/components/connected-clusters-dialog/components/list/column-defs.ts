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

export const columnDefsFn = ($translate: ng.translate.ITranslateService) => [
    {
        name: 'name',
        displayName: $translate.instant('connectedClustersDialog.gridColumnTitles.tag'),
        field: 'name',
        cellTemplate: `
            <div class='ui-grid-cell-contents'>
                <cluster-security-icon
                    secured='row.entity.secured'
                ></cluster-security-icon>
                {{ COL_FIELD }}
            </div>
        `,
        width: 200,
        minWidth: 120
    },
    {
        name: 'id',
        displayName: $translate.instant('connectedClustersDialog.gridColumnTitles.id'),
        field: 'id',
        cellFilter: 'id8',
        cellClass: 'ui-grid-number-cell',
        width: 160,
        minWidth: 110
    },
    {
        name: 'nids',
        displayName: $translate.instant('connectedClustersDialog.gridColumnTitles.nids'),
        field: 'size',
        cellClass: 'ui-grid-number-cell',
        width: 160,
        minWidth: 160
    },
    {
        name: 'status',
        displayName: $translate.instant('connectedClustersDialog.gridColumnTitles.status'),
        field: 'active',
        cellTemplate: `
            <div class='ui-grid-cell-contents ui-grid-cell--status'>
                <connected-clusters-cell-status
                    value='COL_FIELD'
                ></connected-clusters-cell-status>
                <connected-clusters-cell-logout
                    ng-if='row.entity.secured'
                    cluster-id='row.entity.id'
                ></connected-clusters-cell-logout>
            </div>
        `,
        minWidth: 90
    }
];
