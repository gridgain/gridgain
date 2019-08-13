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

import {dropTestDB, insertTestUser, resolveUrl} from '../environment/envtools';
import {createRegularUser} from '../roles';
import * as helpMenu from '../components/helpMenu';
import {userMenu} from '../components/userMenu';
import * as gettingStarted from '../components/gettingStartedDialog';

const user = createRegularUser();

fixture('Help menu')
    .beforeEach(async(t) => {
        await dropTestDB();
        await insertTestUser();
        await t.useRole(user);
    })
    .afterEach(async(t) => {
        await dropTestDB();
    });

test.page(resolveUrl('/'))('Help menu items', async(t) => {
    await t
        .hover(helpMenu.trigger)
        .expect(helpMenu.item('Documentation').getAttribute('href')).eql('https://docs.gridgain.com/docs/web-console')
        .expect(helpMenu.item('Forms').getAttribute('href')).eql('https://forums.gridgain.com/home')
        .expect(helpMenu.item('Support').getAttribute('href')).eql('https://gridgain.freshdesk.com/support/login')
        .expect(helpMenu.item('Webinars').getAttribute('href')).eql('https://www.gridgain.com/resources/webinars')
        .expect(helpMenu.item('Whitepapers').getAttribute('href')).eql('https://www.gridgain.com/resources/literature/white-papers')
        .click(helpMenu.item('Getting Started'))
        .expect(gettingStarted.dialog.exists).ok()
        .click(gettingStarted.closeButton);
    await t
        .hover(userMenu.button)
        .expect(userMenu._selector.find('a').withText('Getting started').exists)
        .notOk('Getting started was moved to help menu');
});
