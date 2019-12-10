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

import { Role, t } from 'testcafe';
import { resolveUrl, insertFullTestUser, dropTestDB } from './environment/envtools';
import {pageSignin as page} from './page-models/pageSignin';

const testUserBase = {
    firstName: 'John',
    lastName: 'Doe',
    company: 'TestCompany',
    country: 'Canada',
    industry: 'Banking'
};

export const randomEmail = () => `testcase+${Math.random()}@example.com`;

export const createRegularUser = async(login = 'a@example.com', password = 'a') => {
    const user = Object.assign({}, testUserBase, { email: login, password: password });

    await dropTestDB(user.email);
    await insertFullTestUser(user);

    return Role(resolveUrl('/signin'), async() => {
        await t.eval(() => window.localStorage.clear());

        // Disable "Getting started" modal.
        await t.eval(() => window.localStorage.showGettingStarted = 'false');
        await page.login(user.email, user.password);
    });
};

const tempUser = async(email = randomEmail()) => {
    return {
        role: await createRegularUser(email),
        dispose: async() => await dropTestDB(email)
    };
};

export const prepareUser = async(t) => {
    const {role, dispose} = await tempUser();
    t.ctx.dispose = dispose;
    await t.useRole(role);
};

export const cleanupUser = async(t) => {
    await t.ctx.dispose();
};
