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
    editModelTitle,
    popoverErrorNotification,
    general,
    sqlQuery,
    cacheStore
} from '../../page-models/pageConfigurationAdvancedModels';
import {createCacheButton} from '../../page-models/pageConfigurationAdvancedCaches'
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

const INVALID_TYPE = '1.type';

const _createCache = async(t) => {
    await t.click(pageAdvancedConfiguration.cachesNavButton)
        .click(createCacheButton);

    await pageAdvancedConfiguration.save();

    await t.click(pageAdvancedConfiguration.modelsNavButton);
};

const _configureMinimalQueryFields = async(t) => {
    await t.click(sqlQuery.fields.addFirstField)
        .typeText(sqlQuery.fields.fieldName.control, 'id')
        .typeText(sqlQuery.fields.fieldClass.control, 'Integer')
        .click(sqlQuery.fields.addNextField)
        .click(sqlQuery.fields.addNextField)
        .typeText(sqlQuery.fields.fieldName.control, 'data')
        .typeText(sqlQuery.fields.fieldClass.control, 'String')
};

const _configureMinimalCacheStore = async(t) => {
    await t.click(cacheStore.panel.heading)
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
};

// Cover 1 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Save valid SQL scheme with empty cache', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.keyType.control, KEY_CLS)
        .typeText(general.valueType.control, VALUE_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(successNotification.withText(`Model "${VALUE_CLS}" saved`)).ok('Success notification on save should be visible')
        .expect(editModelTitle.visible).ok('Page mode should be changed to edit');

    await t.eval(() => window.location.reload());
    await t.expect(editModelTitle.visible).ok('Page mode should be changed to edit');
});

// Cover 2 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Save valid SQL scheme with selected cache', async(t) => {
    await _createCache(t);

    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await general.selectCaches('CacheNames');

    await t.typeText(general.keyType.control, KEY_CLS)
        .typeText(general.valueType.control, VALUE_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(successNotification.withText(`Model "${VALUE_CLS}" saved`)).ok('Success notification on save should be visible')
        .expect(editModelTitle.visible).ok('Page mode should be changed to edit');

    await t.eval(() => window.location.reload());
    await t.expect(editModelTitle.visible).ok('Page mode should be changed to edit');
});

// Cover 3 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Save valid SQL scheme with selected cache and annotations type of metadata', async(t) => {
    await _createCache(t);

    await t.click(createModelButton);

    await _configureMinimalCacheStore(t);

    await general.selectCaches('CacheNames');
    await general.queryMetadata.selectOption('Annotations');

    await t.typeText(general.keyType.control, KEY_CLS)
        .typeText(general.valueType.control, VALUE_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(successNotification.withText(`Model "${VALUE_CLS}" saved`)).ok('Success notification on save should be visible')
        .expect(editModelTitle.visible).ok('Page mode should be changed to edit');

    await t.eval(() => window.location.reload());
    await t.expect(editModelTitle.visible).ok('Page mode should be changed to edit');
});

// Cover 4 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with empty value type', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.keyType.control, KEY_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('required').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 5 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with invalid value type', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.valueType.control, INVALID_TYPE)
        .typeText(general.keyType.control, KEY_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 6 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with empty key type', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.valueType.control, VALUE_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('required').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 7 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with invalid key type', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.valueType.control, VALUE_CLS)
        .typeText(general.keyType.control, INVALID_TYPE);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 8 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with empty key and value types', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.click(cacheStore.dbTable.control);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('required').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 9 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with empty key type and invalid value type', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.valueType.control, INVALID_TYPE);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('required').visible).ok('Validation error message for required field shold be visible')
        .expect(general.valueType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 10 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with invalid key type and empty value type', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.keyType.control, INVALID_TYPE);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible')
        .expect(general.valueType.getError('required').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 11 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with invalid key and value types', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.valueType.control, INVALID_TYPE)
        .typeText(general.keyType.control, INVALID_TYPE);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.keyType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible')
        .expect(general.valueType.getError('javaIdentifier').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 12 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Validation with value Java built-in type', async(t) => {
    await t.click(createModelButton);

    await _configureMinimalQueryFields(t);
    await _configureMinimalCacheStore(t);

    await t.typeText(general.valueType.control, 'java.lang.Long')
        .typeText(general.keyType.control, KEY_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode')
        .expect(general.valueType.getError('javaBuiltInClass').visible).ok('Validation error message for required field shold be visible');

    await t.eval(() => window.location.reload());
    await t.expect(createModelTitle.visible).ok('Page should stay in creation mode');
});

// Cover 13 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test.only('Save SQL scheme with value Java built-in like type', async(t) => {
    await _createCache(t);

    await t.click(createModelButton)
        .click(general.generatePOJOClasses.control);

    await _configureMinimalQueryFields(t);

    await t.typeText(general.valueType.control, 'foo.bar.Long')
        .typeText(general.keyType.control, KEY_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(successNotification.withText(`Model "${VALUE_CLS}" saved`)).ok('Success notification on save should be visible')
        .expect(editModelTitle.visible).ok('Page mode should be changed to edit');

    await t.eval(() => window.location.reload());
    await t.expect(editModelTitle.visible).ok('Page mode should be changed to edit');
});

// Cover 14 testcase of https://ggsystems.atlassian.net/browse/GG-25370
test('Save SQL scheme with value Java built-in type', async(t) => {
    await _createCache(t);

    await t.click(createModelButton)
        .click(general.generatePOJOClasses.control);

    await _configureMinimalQueryFields(t);

    await t.typeText(general.valueType.control, 'java.lang.Long')
        .typeText(general.keyType.control, KEY_CLS);

    await pageAdvancedConfiguration.save();

    await t.expect(successNotification.withText(`Model "${VALUE_CLS}" saved`)).ok('Success notification on save should be visible')
        .expect(editModelTitle.visible).ok('Page mode should be changed to edit');

    await t.eval(() => window.location.reload());
    await t.expect(editModelTitle.visible).ok('Page mode should be changed to edit');
});
