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

import {resolveUrl} from '../../environment/envtools';
import {prepareUser, cleanupUser} from '../../roles';
import {
    agentStat,
    AGENT_ONLY_NO_CLUSTER,
    errorResponseForEventType,
    schemaImportRequest,
    TEST_JDBC_IMPORT_DATA
} from '../../mocks/agentTasks';
import {WebSocketHook} from '../../mocks/WebSocketHook';
import {importDBButton, importDBDialog} from '../../page-models/importFromDatabaseDialog'
import {errorNotification} from '../../components/notifications';

fixture('Import from database dialog')
    .beforeEach(async(t) => {
        await prepareUser(t);
        await t.navigateTo(resolveUrl(`/configuration/overview`))
    })
    .afterEach(async(t) => {
        await cleanupUser(t);
    });

test('Dialog has valid state when JDBC drivers are not available', async(t) => {
    await t.addRequestHooks(
        t.ctx.ws = new WebSocketHook()
            .use(
                agentStat(AGENT_ONLY_NO_CLUSTER),
                errorResponseForEventType('schemaImport:drivers')
            )
    );

    await t.click(importDBButton);
    await t.expect(importDBDialog.dialog.visible).ok('Import from Database dialog should be visible');
    await t.expect(errorNotification.exists).ok('Error notification should be visible');
    await t.expect(importDBDialog.importImpossibleMsg.exists).ok('Steps to fix problem should be visible');
});

test('Dialog has valid state when JDBC drivers are available', async(t) => {
    await t.addRequestHooks(
        t.ctx.ws = new WebSocketHook()
            .use(
                agentStat(AGENT_ONLY_NO_CLUSTER),
                schemaImportRequest(TEST_JDBC_IMPORT_DATA)
            )
    );

    await t.click(importDBButton);
    await t.expect(importDBDialog.dialog.visible).ok('Import from Database dialog should be visible');
    await t.expect(errorNotification.exists).notOk('No error notifications should be visible');
    await t.expect(importDBDialog.driverSelectorField.exists).ok('Driver configuration panel should be visible')
});
