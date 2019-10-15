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

import {dropTestDB, insertTestUser, resolveUrl} from '../../environment/envtools';
import {createRegularUser} from '../../roles';
import {pageAdvancedConfiguration} from '../../components/pageAdvancedConfiguration'
import {createModelButton, general} from '../../page-models/pageConfigurationAdvancedModels';
import {successNotification} from '../../components/notifications';

const regularUser = createRegularUser();

fixture('Advanced SQL scheme configuration')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t
            .useRole(regularUser)
            .navigateTo(resolveUrl('/configuration/new/advanced/models'));
    })
    .after(dropTestDB);

test('Base required fields checked on save.', async(t) => {
    const VALUE_CLS = 'test.cls.name.Value';

    await t.click(createModelButton)
        .click(pageAdvancedConfiguration.saveButton)
        .expect(pageAdvancedConfiguration.fieldErrorNotification.withText('Key type could not be empty!').visible)
        .ok('Error notification for key field should be visible');

    await t.typeText(general.keyType.control, 'test.cls.name.Key')
        .click(pageAdvancedConfiguration.saveButton)
        .expect(pageAdvancedConfiguration.fieldErrorNotification.withText('Value type could not be empty!').visible)
        .ok('Error notification for value field should be visible');

    await t.typeText(general.valueType.control, VALUE_CLS)
        .click(pageAdvancedConfiguration.saveButton)
        .expect(pageAdvancedConfiguration.popoverErrorNotification.withText('SQL query domain model should be configured').visible)
        .ok('Error notification for not configured SQL query section should be visible');

    await general.queryMetadata.selectOption('Annotations');
    await t.click(pageAdvancedConfiguration.saveButton)
        .expect(pageAdvancedConfiguration.popoverErrorNotification.withText('Domain model for cache store should be configured when generation of POJO classes is enabled').visible)
        .ok('Error notification for not configured cache store section should be visible');

    await t.click(general.generatePOJOClasses.control)
        .click(pageAdvancedConfiguration.saveButton)
        .expect(successNotification.withText(`Model "${VALUE_CLS}" saved`).visible).ok('Notification about saved SQL scheme should be shown');
});
