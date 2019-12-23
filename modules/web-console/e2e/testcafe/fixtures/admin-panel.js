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

import { Selector } from 'testcafe';
import { AngularJSSelector } from 'testcafe-angular-selectors';
import { dropTestDB, insertTestUser, resolveUrl } from '../environment/envtools';
import { createRegularUser } from '../roles';
import * as pageListOfUsers from '../page-models/pageAdminListOfRegisteredUsers';
import {PageConfigurationBasic} from "../page-models/PageConfigurationBasic";
import {pageAdvancedConfiguration} from '../components/pageAdvancedConfiguration';
import * as pageAdvancedModel from '../page-models/pageConfigurationAdvancedModels';
import {pageProfile} from '../page-models/pageProfile';
import {successNotification} from "../components/notifications";
import {advancedNavButton} from '../components/pageConfiguration';
import {scrollToPageBottom} from "../helpers";

const regularUser = createRegularUser();

fixture('Checking admin panel')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t.useRole(regularUser);
        await t.navigateTo(resolveUrl('/settings/admin'));
    })
    .after(async() => {
        await dropTestDB();
    });

const setNotificationsButton = Selector('button').withText('Set user notifications');

test('Testing setting notifications', async(t) => {
    await t.click(setNotificationsButton);

    await t
        .expect(Selector('h4').withText(/.*Set user notifications.*/).exists)
        .ok()
        .click('.ace_content')
        .pressKey('ctrl+a delete')
        .pressKey('t e s t space m e s s a g e')
        .click(AngularJSSelector.byModel('$ctrl.visible'))
        .click('#btn-submit');

    await t
        .expect(Selector('.wch-notification').innerText)
        .eql('test message');

    await t.click(setNotificationsButton);

    await t
        .click('.ace_content')
        .pressKey('ctrl+a delete')
        .click(AngularJSSelector.byModel('$ctrl.visible'))
        .click('#btn-submit');

    await t
        .expect(Selector('.wch-notification', { visibilityCheck: false } ).visible)
        .notOk();
});

const _createBaseModel = async(t, keyType, valueType) => {
    await t.click(pageAdvancedConfiguration.modelsNavButton)
        .click(pageAdvancedModel.createModelButton)
        .click(pageAdvancedModel.general.generatePOJOClasses.control);

    await scrollToPageBottom();

    await t.click(pageAdvancedModel.sqlQuery.panel.heading)
        .typeText(pageAdvancedModel.general.keyType.control, keyType, {paste: true})
        .typeText(pageAdvancedModel.general.valueType.control, valueType, {paste: true});

    await pageAdvancedModel.general.queryMetadata.selectOption('Annotations');

    await t.click(pageAdvancedConfiguration.saveButton);
};

test.meta({
    testPlan: 'https://ggsystems.atlassian.net/browse/GG-26393',
    testCase: '1'
})('Validation of user metrics data', async(t) => {
    await t.resizeWindow(1920, 1080);

    const page = new PageConfigurationBasic();

    await scrollToPageBottom();
    await page.cachesList.addItem();
    await scrollToPageBottom();
    await page.cachesList.addItem();

    await page.saveWithoutDownload();

    await t.click(advancedNavButton);
    await _createBaseModel(t, 'test.Key1', 'test.Value1');
    await _createBaseModel(t, 'test.Key2', 'test.Value2');
    await _createBaseModel(t, 'test.Key3', 'test.Value3');

    await pageAdvancedConfiguration.save();

    await t.navigateTo(resolveUrl('/settings/profile'));

    const newFirstName = 'newFirstName';
    const newLastName = 'newLastName';
    const newCompaty = 'newcompaty';
    const newCountry = 'United States';

    await t
        .typeText(pageProfile.firstName.control, newFirstName, {replace: true, paste: true})
        .typeText(pageProfile.lastName.control, newLastName, {replace: true, paste: true})
        .typeText(pageProfile.company.control, newCompaty, {replace: true, paste: true});
    await pageProfile.country.selectOption(newCountry);
    await t.click(pageProfile.saveChangesButton)
        .expect(successNotification.withText('Profile saved.').exists).ok();

    await t.navigateTo(resolveUrl('/settings/admin'));

    const emailFilterSelector = pageListOfUsers.usersTable.findFilter('Email');
    const emailCellSelector = pageListOfUsers.usersTable.findCell(0, 'Email');

    await t.typeText(emailFilterSelector, t.ctx.email, {paste: true})
        .expect(pageListOfUsers.usersTable.findCell(0, 'User').textContent).contains(`${newFirstName} ${newLastName}`)
        .expect(emailCellSelector.textContent).contains(t.ctx.email)
        .expect(pageListOfUsers.usersTable.findCell(0, 'Company').textContent).contains(newCompaty)
        .expect(pageListOfUsers.usersTable.findCell(0, 'Country').textContent).contains('USA');

    const lastLoginStr = await pageListOfUsers.usersTable.findCell(0, 'Last login').textContent;
    const lastActiveStr = await pageListOfUsers.usersTable.findCell(0, 'Last activity').textContent;

    const na = 'N/A';

    await t.expect(lastLoginStr).notEql(na)
        .expect(lastActiveStr).notEql(na);

    const lastLogin = Date.parse(lastLoginStr);
    const lastActive = Date.parse(lastActiveStr);

    const start = t.ctx.startTime;
    const now = Date.now();

    await t.expect(start <= lastLogin && now >= lastLogin).ok()
        .expect(start <= lastActive && now >= lastActive).ok();

    // Check count of user clusters.
    await t.expect(pageListOfUsers.usersTable.findCell(0, 5).textContent).eql('1')
        // Check count of user SQL models.
        .expect(pageListOfUsers.usersTable.findCell(0, 6).textContent).eql('3')
        // Check count of user caches.
        .expect(pageListOfUsers.usersTable.findCell(0, 7).textContent).eql('2');

    await t.navigateTo(resolveUrl('/settings/profile'));

    const newEmail = `1+${t.ctx.email}`;

    await t.typeText(pageProfile.email.control, newEmail, {replace: true, paste: true})
        .click(pageProfile.saveChangesButton)
        .expect(successNotification.withText('Profile saved.').exists).ok();

    t.ctx.email = newEmail;

    await t.navigateTo(resolveUrl('/settings/admin'));

    await t.typeText(emailFilterSelector, newEmail, {paste: true})
        .expect(emailCellSelector.textContent).contains(t.ctx.email)
})
.before(async(t) => {
    const d = new Date();

    d.setSeconds(0, 0);

    t.ctx.startTime = d.getTime();
    t.ctx.email = 'a@example.com';
    t.ctx.user = createRegularUser(t.ctx.email);

    const user = {
        password: 'a',
        email: t.ctx.email,
        firstName: 'John',
        lastName: 'Doe',
        company: 'TestCompany',
        country: 'Canada',
        industry: 'Banking'
    };

    await dropTestDB(t.ctx.email);
    await insertTestUser(user);
    await t.useRole(t.ctx.user)
        .navigateTo(resolveUrl('/configuration/new/basic'))
})
.after(async(t) => {
    await dropTestDB(t.ctx.email);
});
