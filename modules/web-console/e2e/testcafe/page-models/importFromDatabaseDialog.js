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

import {Selector} from 'testcafe';

export const importDBButton = Selector('.btn-ignite').withText('Import from Database');

export const _importDBDialog = Selector('h4').withText('Import domain models from database').parent('.modal-dialog');

export const importDBDialog = {
    dialog: _importDBDialog,
    importImpossibleMsg: _importDBDialog.find('div').withText('Domain model could not be imported'),
    driverSelectorField: _importDBDialog.find('#jdbcDriverJarInput')
};
