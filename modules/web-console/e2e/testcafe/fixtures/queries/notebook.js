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
import {WebSocketHook} from '../../mocks/WebSocketHook';
import {
    cacheNamesCollectorTask, agentStat, simeplFakeSQLQuery, foreverExecutingQuery,
    FAKE_CLUSTERS, SIMPLE_QUERY_RESPONSE, FAKE_CACHES, INACTIVE_CLUSTER
} from '../../mocks/agentTasks';
import {resolveUrl, dropTestDB, insertTestUser} from '../../environment/envtools';
import {createRegularUser} from '../../roles';
import {
    Paragraph, showQueryDialog, confirmClearQueryDialog, renameQueryDialog, paragraphPanels,
    addScanQueryButton
} from '../../page-models/pageQueryNotebook';
import {PageQueriesNotebooksList} from '../../page-models/PageQueries';

const user = createRegularUser();
const notebooks = new PageQueriesNotebooksList();
const query = `SELECT * FROM Person;`;
const paragraph = new Paragraph('Query');

fixture('Notebook')
    .beforeEach(async(t) => {
        await dropTestDB();
        await insertTestUser();
    })
    .afterEach(async(t) => {
        t.ctx.ws.destroy();
        await dropTestDB();
    });

test('With inactive cluster', async(t) => {
    await t.addRequestHooks(
        t.ctx.ws = new WebSocketHook()
            .use(
                agentStat(INACTIVE_CLUSTER)
            )
    );

    await t
        .useRole(user)
        .navigateTo(resolveUrl('/queries/notebooks'));
    await notebooks.createNotebook('Foo');
    await t.click(notebooks.getNotebookByName('Foo'));
    await paragraph.enterQuery(query, {replace: true});
    await t.expect(paragraph.topRightExecuteButton.withAttribute('disabled')).ok('Top right Execute button should be disabled');
    await t.expect(paragraph.bottomExecuteButton.withAttribute('disabled')).ok('Bottom Execute button should be disabled');
});

test('Sending a request', async(t) => {
    await t.addRequestHooks(
        t.ctx.ws = new WebSocketHook()
            .use(
                agentStat(FAKE_CLUSTERS),
                cacheNamesCollectorTask(FAKE_CACHES),
                simeplFakeSQLQuery(_.first(_.keys(FAKE_CLUSTERS.clusters[0].nodes)), SIMPLE_QUERY_RESPONSE)
            )
    );

    await t
		.useRole(user)
        .navigateTo(resolveUrl('/queries/notebooks'));
    await notebooks.createNotebook('Foo');
    await t.click(notebooks.getNotebookByName('Foo'));
    await paragraph.enterQuery(query, {replace: true});
    await t
        .click(paragraph.bottomExecuteButton)
        .pressKey('pagedown')
        .expect(paragraph.resultsTable._selector.visible).ok()
        .expect(paragraph.resultsTable.findCell(0, 'ID').innerText).contains(SIMPLE_QUERY_RESPONSE.result.rows[0][0].toString())
        .expect(paragraph.resultsTable.findCell(0, 'NAME').innerText).contains(SIMPLE_QUERY_RESPONSE.result.rows[0][1])
        .expect(paragraph.resultsTable.findCell(1, 'ID').innerText).contains(SIMPLE_QUERY_RESPONSE.result.rows[1][0].toString())
        .expect(paragraph.resultsTable.findCell(1, 'NAME').innerText).contains(SIMPLE_QUERY_RESPONSE.result.rows[1][1])
        .expect(paragraph.resultsTable.findCell(2, 'ID').innerText).contains(SIMPLE_QUERY_RESPONSE.result.rows[2][0].toString())
        .expect(paragraph.resultsTable.findCell(2, 'NAME').innerText).contains(SIMPLE_QUERY_RESPONSE.result.rows[2][1])
        .click(paragraph.showQueryButton)
        .expect(showQueryDialog.body.innerText).contains(query)
        .expect(showQueryDialog.footer.innerText).contains('Duration: 0')
        .click(showQueryDialog.okButton)
        .click(paragraph.clearResultButton)
        .click(confirmClearQueryDialog.confirmButton)
        .expect(paragraph.resultsTable._selector.exists).notOk();
});

// Regression test for https://ggsystems.atlassian.net/browse/GG-23314
test('Very long query name', async(t) => {
    await t.addRequestHooks(
        t.ctx.ws = new WebSocketHook()
            .use(
                agentStat(FAKE_CLUSTERS),
                cacheNamesCollectorTask(FAKE_CACHES),
            )
    );

    await t
        .useRole(user)
        .navigateTo(resolveUrl('/queries/notebooks'));

    const veryLongName = 'Foo '.repeat(30);
    const notebookName = 'Test';
    await notebooks.createNotebook(notebookName);
    await t.click(notebooks.getNotebookByName(notebookName));
    const query = new Paragraph('Query');
    const oldWidth = await query.body.clientWidth;
    await t.click(query.moreQueryActionsButton);
    await t.click(query.renameQueryButton);
    await t.typeText(renameQueryDialog.input.control, veryLongName, {replace: true});
    await t.click(renameQueryDialog.confirmButton);
    await t.expect(paragraphPanels.nth(0).clientWidth).eql(oldWidth, 'Panel width should not depend on query name length');
});

test('Cancel query button', async(t) => {
    await t
        .addRequestHooks(
            t.ctx.ws = new WebSocketHook()
                .use(
                    agentStat(FAKE_CLUSTERS),
                    cacheNamesCollectorTask(FAKE_CACHES),
                    foreverExecutingQuery(_.first(_.keys(FAKE_CLUSTERS.clusters[0].nodes)))
                )
        )
        .resizeWindow(1130, 800)
        .useRole(user)
        .navigateTo(resolveUrl('/queries/notebooks'));
    await notebooks.createNotebook('Foo');
    await t.click(notebooks.getNotebookByName('Foo'));

    // SQL query
    await paragraph.enterQuery(query, {replace: true});
    const oldFlagsPosition = await paragraph.queryFlags.boundingClientRect;
    await t
        .click(paragraph.bottomExecuteButton)
        .expect(paragraph.cancelQueryButton.visible).ok('Cancel button is visible while query is running')
        // Regression test for https://ggsystems.atlassian.net/browse/GG-23314
        .expect(paragraph.queryFlags.boundingClientRect).eql(oldFlagsPosition, 'Cancel button should not change query panel height')
        .click(paragraph.cancelQueryButton)
        .expect(paragraph.cancelQueryButton.exists).notOk('SQL query cancel button is hidden after query is cancelled')
        .click(paragraph.title);

    // Scan query
    const scan = new Paragraph('Scan1');
    await t
        .click(addScanQueryButton)
        .click(scan.topRightScanButton)
        .expect(scan.cancelQueryButton.visible).ok('Scan query cancel button is visible while query is running')
        .click(scan.cancelQueryButton)
        .expect(scan.cancelQueryButton.exists).notOk('Scan query cancel button is hidden after query is cancelled');
});
