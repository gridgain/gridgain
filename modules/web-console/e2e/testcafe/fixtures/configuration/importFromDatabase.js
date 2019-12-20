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

import _ from 'lodash';
import {dropTestDB, insertTestUser, resolveUrl} from '../../environment/envtools';
import {createRegularUser} from '../../roles';
import {
    agentStat,
    AGENT_ONLY_NO_CLUSTER,
    errorResponseForEventType,
    schemaImportRequest,
    TEST_JDBC_IMPORT_DATA,
    FULL_LIST_COLUMN_TYPES
} from '../../mocks/agentTasks';
import {WebSocketHook} from '../../mocks/WebSocketHook';
import {importDBButton, importDBDialog} from '../../page-models/importFromDatabaseDialog'
import {errorNotification} from '../../components/notifications';
import {PageConfigurationOverview} from '../../page-models/PageConfigurationOverview';
import {PageConfigurationBasic} from '../../page-models/PageConfigurationBasic';
import {advancedSqlSchemeMenu, cacheStore} from '../../page-models/pageConfigurationAdvancedModels';
import {previewDialog} from '../../page-models/previewProjectDialog';

const overviewPage = new PageConfigurationOverview();

fixture('Import from database dialog')
    .beforeEach(async(t) => {
        await dropTestDB();
        await insertTestUser();
        await t.useRole(await createRegularUser()).navigateTo(resolveUrl(`/configuration/overview`))
    })
    .afterEach(async(t) => {
        await dropTestDB();
        t.ctx.ws.destroy();
    })
    .after(async(t) => await dropTestDB());

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

test('Base check of model import and project preview', async(t) => {
    const testPackageName = 'test_package';

    await t.click(importDBButton)
        .expect(importDBDialog.dialog.visible).ok('Import from Database dialog should be visible')
        .click(importDBDialog.nextAction)
        .click(importDBDialog.nextAction)
        .click(importDBDialog.nextAction)
        .typeText(importDBDialog.packageNameInput, testPackageName, {paste: true, replace: true})
        .click(importDBDialog.saveAction);

    await overviewPage.clustersTable.toggleRowSelection(1);
    await overviewPage.clustersTable.performAction('Edit');

    const page = new PageConfigurationBasic();

    await t.click(page.buttonPreviewProject)
        .expect(previewDialog.dialog.visible).ok()
        .expect(previewDialog.loadingOverview.visible).notOk()
        .click(previewDialog.treeLabel.withText(testPackageName))
        .expect(previewDialog.treeLabel.withText('Test.java').exists).ok()
        .click(previewDialog.treeLabel.withText('pom.xml'))
        .click(previewDialog.textComponent)
        .pressKey('pagedown')
        .expect(previewDialog.textContent.textContent).contains('<version>test.version</version>');
})
.before(async(t) => {
    await dropTestDB();
    await insertTestUser();

    await t.addRequestHooks(
        t.ctx.ws = new WebSocketHook()
            .use(
                agentStat(AGENT_ONLY_NO_CLUSTER),
                schemaImportRequest(TEST_JDBC_IMPORT_DATA)
            )
    );

    await t.useRole(await createRegularUser()).navigateTo(resolveUrl(`/configuration/overview`));
});

test('Base check of model import or different data types', async(t) => {
    await t.click(importDBButton)
        .expect(importDBDialog.dialog.visible).ok('Import from Database dialog should be visible')
        .click(importDBDialog.nextAction)
        .click(importDBDialog.nextAction)
        .click(importDBDialog.nextAction)
        .click(importDBDialog.saveAction);

    await overviewPage.clustersTable.toggleRowSelection(1);
    await overviewPage.clustersTable.performAction('Edit');

    const page = new PageConfigurationBasic();

    await t.click(page.advancedPresentation)
        .click(advancedSqlSchemeMenu)
        .click(cacheStore.panel.heading);

    for (let i = 0; i < t.ctx.importData.tables[0].columns.length; i++) {
        const col = t.ctx.importData.tables[0].columns[i];

        await t.expect(cacheStore.fieldViewItem.withText(`${(col.expectedType === 'byte[]' ? 'fieldByte[]col' + i : col.name)} / ${col.expectedType}`).exists).ok();
    }
})
.before(async(t) => {
    await dropTestDB();
    await insertTestUser();

    t.ctx.importData = _.cloneDeep(TEST_JDBC_IMPORT_DATA);
    t.ctx.importData.tables[0].columns = [];

    _.forEach(FULL_LIST_COLUMN_TYPES, (type, idx) => {
        t.ctx.importData.tables[0].columns.push({
            name: type.expectedType.toLowerCase() + 'col' + idx,
            type: type.type,
            unsigned: type.unsigned || false,
            key: idx === 0,
            nullable: false,
            expectedType: type.expectedType
        })
    });

    await t.addRequestHooks(
        t.ctx.ws = new WebSocketHook()
            .use(
                agentStat(AGENT_ONLY_NO_CLUSTER),
                schemaImportRequest(t.ctx.importData)
            )
    );

    await t.useRole(await createRegularUser()).navigateTo(resolveUrl(`/configuration/overview`));
});
