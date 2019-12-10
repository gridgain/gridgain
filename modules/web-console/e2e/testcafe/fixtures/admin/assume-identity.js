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

import { dropTestDB, insertFullTestUser, resolveUrl } from '../../environment/envtools';
import { prepareUser, cleanupUser } from '../../roles';
import * as admin from '../../page-models/pageAdminListOfRegisteredUsers';
import {pageProfile as profile} from '../../page-models/pageProfile';
import * as notifications from '../../components/permanentNotifications';
import {userMenu} from '../../components/userMenu';

fixture('Assumed identity')
    .before(async() => {
        await dropTestDB(admin.TEST_USER.email);
        await insertFullTestUser(admin.TEST_USER);
    })
    .beforeEach(async(t) => {
        await prepareUser(t);
    })
    .afterEach(async(t) => {
        await dropTestDB(admin.TEST_USER.email);
        await cleanupUser(t);
    });

test('Become user', async(t) => {
    await t.navigateTo(resolveUrl('/settings/admin'));
    await t.typeText(admin.usersTable.findFilter('Email'), admin.TEST_USER.email, {paste: true});
    await t.click(admin.userNameCell.withText('User Name'));
    await admin.usersTable.performAction('Become this user');
    await t
        .hover(userMenu.button)
        // See https://ggsystems.atlassian.net/browse/GG-24068
        .expect(userMenu.menuItem.withText('Admin panel').exists).notOk()
        .expect(userMenu.menuItem.withText('Log out').exists).notOk();
    await userMenu.clickOption('Profile');
    await t
        .expect(profile.firstName.control.value).eql(admin.TEST_USER.firstName)
        .expect(profile.lastName.control.value).eql(admin.TEST_USER.lastName)
        .expect(profile.email.control.value).eql(admin.TEST_USER.email)
        .expect(profile.country.control.value).eql(admin.TEST_USER.country)
        .expect(profile.company.control.value).eql(admin.TEST_USER.company)
        .expect(notifications.assumedUserFullName.innerText).eql('User Name')
        .typeText(profile.firstName.control, '1')
        .click(profile.saveChangesButton)
        .expect(notifications.assumedIdentityNotification.visible).ok()
        .click(notifications.revertIdentityButton)
        .hover(userMenu.button)
        // See https://ggsystems.atlassian.net/browse/GG-24068
        .expect(userMenu.menuItem.withText('Admin panel').count).eql(1);
});
