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

import {WebSocketHook} from '../../mocks/WebSocketHook';
import {createRegularUser} from '../../roles';
import {resolveUrl} from '../../environment/envtools';
import {Paragraph} from '../../page-models/pageQueryNotebook';
import {errorNotification} from '../../components/notifications';

const me = createRegularUser('iborisov+1@gridgain.com', '1');
const ws = new WebSocketHook();

fixture('Notebook').requestHooks(ws).after(async() => ws.destroy());

test('Sending a request', async(t) => {
    const query = `SELECT * FROM Person;`;
    const paragraph = new Paragraph('Query');

    await t
		.useRole(me)
		.navigateTo(resolveUrl('/notebook/5cc7ef443787c733b81ce1a5'))
		.click(paragraph.queryField.with({timeout: 0}))
		.typeText(paragraph.queryField, 'A', {modifiers: {ctrl: true}})
		.typeText(paragraph.queryField, query, {replace: true})
		.click(paragraph.executeButton)
		.expect(errorNotification.withText('Failed to execute request on cluster').exists).ok();
});
