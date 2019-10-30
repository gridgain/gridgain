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

import {Selector, t} from 'testcafe';
import {FormField} from '../components/FormField';
import {isVisible} from '../helpers';
import {PanelCollapsible} from '../components/PanelCollapsible';

export const createModelButton = Selector('pc-items-table footer-slot .link-success').filter(isVisible);
export const createModelTitle = Selector('h2').withText('Create model');
export const editModelTitle = Selector('h2').withText('Edit model');
export const popoverErrorNotification = Selector('.popover.validation-error');

export const general = {
    generatePOJOClasses: new FormField({id: 'generatePojoInput'}),
    queryMetadata: new FormField({id: 'queryMetadataInput'}),
    keyType: new FormField({id: 'keyTypeInput'}),
    valueType: new FormField({id: 'valueTypeInput'}),
    panel: new PanelCollapsible('General'),
    selectCaches: async(...cacheNames) => {
        const caches = new FormField({id: 'cachesInput'});
        const cacheSelector = Selector('li').find('button');

        await t.click(caches.control);

        for (let i = 0; i < cacheNames.length; i++)
            await t.click(cacheSelector);

        await t.pressKey('esc');
    }
};

const _sqlFieldsPanel = Selector('label').withText('Fields:').parent('.ignite-form-field');

export const sqlQuery = {
    panel: new PanelCollapsible('Domain model for SQL query'),
    table: new FormField({id: 'tableNameInput'}),
    keyField: new FormField({id: 'keyFieldNameInput'}),
    valueField: new FormField({id: 'valueFieldNameInput'}),
    fields: {
        addFirstField: _sqlFieldsPanel.find('a'),
        addNextField: _sqlFieldsPanel.find('button').withText('Add new field to query'),
        fieldName: new FormField({id: 'nameInput'}),
        fieldClass: new FormField({id: 'classNameInput'}),
        defaultValue: new FormField({id: 'defaultValueInput'}),
        notNull: new FormField({id: 'notNullInput'})
    },
    keyFields: new FormField({id: 'queryKeyFieldsInput'}),
};

const cacheStoreFieldPanel = (label) => {
    const _panel = Selector('label').withText(label).parent('.ignite-form-field');

    return {
        addField: _panel.find('a'),
        dbName: new FormField({id: 'databaseFieldNameInput'}),
        dbType: new FormField({id: 'databaseFieldTypeInput'}),
        javaName: new FormField({id: 'javaFieldNameInput'}),
        javaType: new FormField({id: 'javaFieldTypeInput'})
    }
};

export const cacheStore = {
    panel: new PanelCollapsible('Domain model for cache store'),
    dbSchema: new FormField({id: 'databaseSchemaInput'}),
    dbTable: new FormField({id: 'databaseTableInput'}),
    keyFields: cacheStoreFieldPanel('Key fields'),
    valueFields: cacheStoreFieldPanel('Value fields')
};
