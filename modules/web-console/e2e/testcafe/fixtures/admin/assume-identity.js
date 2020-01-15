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

import { dropTestDB, insertTestUser, resolveUrl } from '../../environment/envtools';
import { createRegularUser } from '../../roles';
import * as admin from '../../page-models/pageAdminListOfRegisteredUsers';
import {pageProfile as profile} from '../../page-models/pageProfile';
import * as notifications from '../../components/permanentNotifications';
import {userMenu} from '../../components/userMenu';
import { Selector } from 'testcafe';
import { successNotification } from '../../components/notifications';
import {confirmation} from '../../components/confirmation'

const regularUser = createRegularUser();

const _checkListOfActions = async(t, actions, excluded) => {
    const actionSelector = Selector('.dropdown-menu a');

    for (let i = 0; i < actions.length; i++) {
        const actionLbl = actions[i];

        await t.expect(actionSelector.withText(actionLbl).exists).notEql(excluded);
    }

    return false;
};

const actions = {
    alwaysAvailable: ['Add user'],
    curUserSelectedOnly: ['Activity details'],
    otherUserActions: ['Become this user', 'Revoke admin', 'Remove user']
};

const _checkActionSet = async(t, excludeOtherActions = false, excludeCurOnly = false) => {
    await _checkListOfActions(t, actions.alwaysAvailable);
    await _checkListOfActions(t, actions.curUserSelectedOnly, excludeCurOnly);
    await _checkListOfActions(t, actions.otherUserActions, excludeOtherActions);
};

const CUR_USER = 'John Doe';
const OTHER_USER = 'User Name';

const LONG_TIMEOUT_OPTION = { timeout: 15000 };

fixture('Assumed identity')
    .beforeEach(async(t) => {
        await dropTestDB();
        await dropTestDB(admin.TEST_USER.email);
        await insertTestUser();
        await insertTestUser(admin.TEST_USER);
        await t.useRole(regularUser);
    })
    .afterEach(async() => {
        await dropTestDB(admin.TEST_USER.email);
        await dropTestDB();
    });

test('Become user', async(t) => {
    await t.navigateTo(resolveUrl('/settings/admin'));
    await t.click(admin.userNameCell.withText(OTHER_USER));
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
        .expect(notifications.assumedUserFullName.innerText).eql(OTHER_USER)
        .typeText(profile.firstName.control, '1')
        .click(profile.saveChangesButton)
        .expect(notifications.assumedIdentityNotification.visible).ok()
        .click(notifications.revertIdentityButton)
        .hover(userMenu.button)
        // See https://ggsystems.atlassian.net/browse/GG-24068
        .expect(userMenu.menuItem.withText('Admin panel').count).eql(1);
});

test("Action availability", async(t) => {
    const _changeUserSelection = async(userName) => {
        await t.click(admin.userNameCell.withText(userName))
            .hover(admin.usersTable.actionsButton);
    };

    await t.navigateTo(resolveUrl('/settings/admin'))
        .hover(admin.usersTable.actionsButton);

    await _checkActionSet(t, true, true);

    await admin.usersTable.performAction('Add user');

    const createUserDialog = Selector('.modal-dialog');

    await t.expect(createUserDialog.find('.modal-header').withText('Create User').exists).ok()
        .click(createUserDialog.find('button').withText('Cancel'))
        .hover(admin.usersTable.actionsButton);

    await _checkActionSet(t, true, true);

    await _changeUserSelection(CUR_USER);

    await _checkActionSet(t, true, false);

    await _changeUserSelection(CUR_USER);

    await _checkActionSet(t, true, true);

    await _changeUserSelection(OTHER_USER);

    await _checkActionSet(t, false, false);

    await _changeUserSelection(OTHER_USER);

    await _checkActionSet(t, true, true);

    await _changeUserSelection(OTHER_USER);

    await admin.usersTable.performAction('Revoke admin');

    await t.expect(successNotification.withText(`Admin rights was successfully revoked for user: "${OTHER_USER}"`).exists).ok();

    await t.eval(() => window.location.reload());
    await t.expect(admin.userNameCell.withText(OTHER_USER).find('i.icon-user').exists).ok(LONG_TIMEOUT_OPTION);

    await _changeUserSelection(OTHER_USER);
    await admin.usersTable.performAction('Grant admin');

    await t.expect(successNotification.withText(`Admin rights was successfully granted for user: "${OTHER_USER}"`).exists).ok();

    await t.eval(() => window.location.reload());
    await t.expect(admin.userNameCell.withText(OTHER_USER).find('i.icon-admin').exists).ok(LONG_TIMEOUT_OPTION);
});

test("Remove user", async(t) => {
    await t.navigateTo(resolveUrl('/settings/admin'))
        .click(admin.userNameCell.withText(OTHER_USER));

    await admin.usersTable.performAction('Remove user');

    await t.expect(confirmation.body.textContent).contains(`Are you sure you want to remove user: "${OTHER_USER}"?`);

    await confirmation.cancel();

    await t.expect(admin.userNameCell.withText(OTHER_USER).exists).ok();

    await admin.usersTable.performAction('Remove user');

    await t.expect(confirmation.body.textContent).contains(`Are you sure you want to remove user: "${OTHER_USER}"?`);

    await confirmation.confirm();

    await t.expect(successNotification.withText(`User has been removed: "${OTHER_USER}"`).exists).ok();

    await t.expect(admin.userNameCell.withText(OTHER_USER).exists).notOk();

    await t.hover(admin.usersTable.actionsButton);

    await _checkActionSet(t, true, true);

    await t.eval(() => window.location.reload());

    await t.expect(admin.userNameCell.withText(OTHER_USER).exists).notOk(LONG_TIMEOUT_OPTION);
});
