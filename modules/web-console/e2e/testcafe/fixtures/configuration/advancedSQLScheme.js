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
import {
    createModelButton,
    createModelTitle,
    popoverErrorNotification,
    general,
    sqlQuery,
    cacheStore
} from '../../page-models/pageConfigurationAdvancedModels';
import {successNotification} from '../../components/notifications';

const regularUser = createRegularUser();

const KEY_CLS = 'test.cls.name.Key';
const VALUE_CLS = 'test.cls.name.Value';

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
    await t.click(createModelButton)
        .click(pageAdvancedConfiguration.saveButton)
        .expect(general.keyType.getError('required').visible).ok('Error notification for key field should be visible');

    await t.typeText(general.keyType.control, KEY_CLS)
        .click(pageAdvancedConfiguration.saveButton)
        .expect(general.valueType.getError('required').visible).ok('Error notification for value field should be visible');

    await t.typeText(general.valueType.control, VALUE_CLS)
        .click(pageAdvancedConfiguration.saveButton)
        .expect(popoverErrorNotification.withText('SQL query domain model should be configured').visible)
        .ok('Error notification for not configured SQL query section should be visible');

    await general.queryMetadata.selectOption('Annotations');
    await t.click(pageAdvancedConfiguration.saveButton)
        .expect(popoverErrorNotification.withText('Domain model for cache store should be configured when generation of POJO classes is enabled').visible)
        .ok('Error notification for not configured cache store section should be visible');

    await t.click(general.generatePOJOClasses.control)
        .click(pageAdvancedConfiguration.saveButton)
        .expect(successNotification.withText(`Model "${VALUE_CLS}" saved`).visible).ok('Notification about saved SQL scheme should be shown');
});

// Cover 4-12 testcases of https://ggsystems.atlassian.net/browse/GG-25370
test.only('Check validation of required fields.', async(t) => {
    const INVALID_TYPE = '1.type';

    // Configure base data for invalid SQL scheme save
    await t.click(createModelButton)
        .typeText(general.keyType.control, 'test.cls.name.Key')
        .click(sqlQuery.fields.addFirstField)
        .typeText(sqlQuery.fields.fieldName.control, 'id')
        .typeText(sqlQuery.fields.fieldClass.control, 'Integer')
        .click(sqlQuery.fields.addNextField)
        .click(sqlQuery.fields.addNextField)
        .typeText(sqlQuery.fields.fieldName.control, 'data')
        .typeText(sqlQuery.fields.fieldClass.control, 'String')
        .click(cacheStore.panel.heading)
        .typeText(cacheStore.dbSchema.control, 'schema')
        .typeText(cacheStore.dbTable.control, 'table')
        .click(cacheStore.keyFields.addField)
        .typeText(cacheStore.keyFields.dbName.control, 'ID')
        .typeText(cacheStore.keyFields.javaName.control, 'id');

    await cacheStore.keyFields.dbType.selectOption('NUMERIC');
    await cacheStore.keyFields.javaType.selectOption('BigDecimal');

    await t.click(cacheStore.valueFields.addField)
        .typeText(cacheStore.valueFields.dbName.control, 'DATA')
        .typeText(cacheStore.valueFields.javaName.control, 'data');

    await cacheStore.valueFields.dbType.selectOption('VARCHAR');
    await cacheStore.valueFields.javaType.selectOption('String');

    await t.click(general.valueType.control);
    await t.eval(() => window.scrollTo(0, 300));

    // Save with empty value type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('required').visible).ok('Validation error message for required field shold be visible')
        .typeText(general.valueType.control, INVALID_TYPE)
        .pressKey('esc');

    // Save with invalid value type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible')
        .selectText(general.valueType.control).pressKey("delete")
        .pressKey('esc')
        .typeText(general.valueType.control, VALUE_CLS)
        .pressKey('esc')
        .selectText(general.keyType.control).pressKey("delete")
        .pressKey('esc');

    // Save with empty key type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('required').visible).ok('Validation error message for required field shold be visible')
        .typeText(general.keyType.control, INVALID_TYPE)
        .pressKey('esc');

    // Save with invalid key type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible')
        .selectText(general.keyType.control).pressKey("delete")
        .pressKey('esc')
        .selectText(general.valueType.control).pressKey("delete")
        .pressKey('esc');

    // Save with empty key type and value type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('required').visible).ok('Validation error message for required field shold be visible')
        .expect(general.keyType.getError('required').visible).ok('Validation error message for required field shold be visible')
        .typeText(general.valueType.control, INVALID_TYPE)
        .pressKey('esc');

    // Save with empty key type and invalid value type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible')
        .typeText(general.keyType.control, INVALID_TYPE)
        .pressKey('esc')
        .selectText(general.valueType.control).pressKey("delete")
        .pressKey('esc');

    // Save with invalid key type and empty value type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible')
        .pressKey('esc')
        .typeText(general.valueType.control, INVALID_TYPE)
        .pressKey('esc');

    // Save with invalid key type and value type.
    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible')
        .expect(general.keyType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});
