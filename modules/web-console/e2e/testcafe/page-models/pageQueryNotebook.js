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
import {PanelCollapsible} from '../components/PanelCollapsible';
import {Table} from '../components/Table';
import {ace, enterAceText} from '../components/ace';
import {FormField} from '../components/FormField';

export const addSqlQueryButton = Selector('button').withText('Add query');
export const addScanQueryButton = Selector('button').withText('Add scan');

export class Paragraph extends PanelCollapsible {
    constructor(title) {
        super(title);

        this.topRightExecuteButton = this.actions.find('query-actions-button').find('button').withExactText('Execute');
        this.bottomExecuteButton = this._selector.find('.sql-controls').find('button').withExactText('Execute');
        this.topRightScanButton = this.actions.find('query-actions-button').find('button').withExactText('Scan');
        this.resultsTable = new Table(this.body.find('.table'));
        this.queryField = ace(this.body);
        this.showQueryButton = this.body.find('a').withExactText('Show query');
        this.clearResultButton = this.body.find('i.fa.fa-eraser');
        this.showStacktraceButton = this.body.find('a').withExactText('Show more');
        this.cancelQueryButton = this._selector.find('button').withExactText('Cancel');
        this.moreQueryActionsButton = this.actions.find('query-actions-button .fa-caret-down').parent('button');
        this.renameQueryButton = this.actions.find('a').withText('Rename');
        this.sqlControls = this._selector.find('.sql-controls');
        this.queryFlags = this.sqlControls.child().nth(2);
    }
    async enterQuery(text, options = {replace: false}) {
        return await enterAceText(this.queryField.with({timeout: 0}), text, options);
    }
}

const showQueryDialogSelector = Selector('.modal-header').withText('SQL query').parent('.modal');

export const showQueryDialog = {
    dialog: showQueryDialogSelector,
    body: showQueryDialogSelector.find('.modal-body'),
    footer: showQueryDialogSelector.find('.modal-footer'),
    okButton: showQueryDialogSelector.find('button').withExactText('Ok')
};

const confirmClearQueryDialogSelector = Selector('.modal-header').withText('Confirm').parent('.modal');

export const confirmClearQueryDialog = {
    dialog: confirmClearQueryDialogSelector,
    confirmButton: confirmClearQueryDialogSelector.find('button').withExactText('Confirm')
};

const showStacktraceDialogSelector = Selector('.modal-header').withText('Error details').parent('.modal');
const stacktraceDialogRootCause = showStacktraceDialogSelector.find('.stacktrace-viewer__cause');
const stacktraceDialogRootCauseLine = showStacktraceDialogSelector.find('.stacktrace-viewer__trace');

export const showStacktraceDialog = {
    dialog: showStacktraceDialogSelector,
    rootCause: stacktraceDialogRootCause,
    rootCauseMsg: stacktraceDialogRootCause.find('span'),
    rootCauseFirstStacktraceLine: stacktraceDialogRootCauseLine,
    causeWithoutStacktrace: showStacktraceDialogSelector.find('.stacktrace-viewer__cause').withText('Cause without stacktrace'),
    downloadLink: showStacktraceDialogSelector.find('span').withText('Full stacktrace is not available'),
    okButton: showStacktraceDialogSelector.find('button').withExactText('OK')
};

const renameQueryDialogSelector = Selector('.modal-header').withText('Rename Query').parent('.modal');

export const renameQueryDialog = {
    dialog: renameQueryDialogSelector,
    input: new FormField({label: 'New query name'}),
    confirmButton: renameQueryDialogSelector.find('button').withText('Confirm')
};

export const paragraphPanels = Selector('queries-notebook panel-collapsible');
