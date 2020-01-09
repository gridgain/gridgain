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

export const categoriesFn = ($translate: ng.translate.ITranslateService) => {
    const translate = (category) => $translate.instant('admin.listOfRegisteredUsers.categoryDisplayNames.' + category);

    return [
        {name: translate('Actions'), visible: false, enableHiding: false},
        {name: translate('user'), visible: true, enableHiding: false},
        {name: translate('email'), visible: true, enableHiding: true},
        {name: translate('activated'), visible: false, enableHiding: true},
        {name: translate('company'), visible: true, enableHiding: true},
        {name: translate('country'), visible: true, enableHiding: true},
        {name: translate('lastLogin'), visible: true, enableHiding: true},
        {name: translate('lastActivity'), visible: true, enableHiding: true},
        {name: translate('failedLoginAttempts'), visible: true, enableHiding: true},
        {name: translate('lastFailedLogin'), visible: false, enableHiding: true},
        {name: translate('configurations'), visible: true, enableHiding: true},
        {name: translate('totalActivities'), visible: false, enableHiding: true},
        {name: translate('configurationActivities'), visible: false, enableHiding: true},
        {name: translate('queriesActivities'), visible: false, enableHiding: true}
    ];
};
