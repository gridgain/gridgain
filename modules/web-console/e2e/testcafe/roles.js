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

/**
 * Create test user and execute login action.
 *
 * @param {string} login Email for user registration.
 * @param {string} password Password for user registration.
 * @return {Promise<Role>} Role to use in testcafe tests.
 */
export const createUserRole = async(login = 'a@example.com', password = 'a') => {
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
        role: await createUserRole(email),
        dispose: async() => await dropTestDB(email)
    };
};

/**
 * Prepare new user to use in testcafe test.
 *
 * @param t Test controller.
 * @param {string} email Email for user creation.
 */
export const prepareUser = async(t, email) => {
    const {role, dispose} = await tempUser(email);
    t.ctx.dispose = dispose;
    await t.useRole(role);
};

/**
 * Remove user that was created for specified test controller.
 *
 * @param t Test controller.
 */
export const cleanupUser = async(t) => {
    await t.ctx.dispose();
};
