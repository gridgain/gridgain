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
import {pageSignin} from '../../page-models/pageSignin';
import {userMenu} from '../../components/userMenu';
import {WebSocketHook} from "../../mocks/WebSocketHook";

fixture('Login')
    .page(resolveUrl('/signin'))
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        t.ctx.touched = false;

        await t.addRequestHooks(t.ctx.ws = new WebSocketHook().use(() => t.ctx.touched = true));
    })
    .after(async() => {
        await dropTestDB();
    })
    .afterEach(async(t) => {
        t.ctx.ws.destroy();
    });

test('Ivalid email', async(t) => {
    await t
        .typeText(pageSignin.email.control, 'aa')
        .typeText(pageSignin.password.control, 'wrong', {replace: true})
        .click(pageSignin.signinButton)
        .expect(pageSignin.email.getError('email').exists).ok('Marks field as invalid');

    await t
        .wait(1000)
        .expect(t.ctx.touched).eql(false, 'Check that web socket not created');
});

test('Unknown email', async(t) => {
    await t
        .typeText(pageSignin.email.control, 'nonexisting@example.com', {replace: true})
        .typeText(pageSignin.password.control, 'wrong', {replace: true})
        .click(pageSignin.signinButton)
        .expect(pageSignin.email.getError('server').exists).ok('Marks input as server-invalid');

    await t
        .wait(1000)
        .expect(t.ctx.touched).eql(false, 'Check that web socket not created');
});

test('Successful login', async(t) => {
    // Disable "Getting started" modal.
    await t.eval(() => window.localStorage.showGettingStarted = 'false');

    await t
        .typeText(pageSignin.email.control, 'a@example.com', {replace: true})
        .typeText(pageSignin.password.control, 'a', {replace: true})
        .click(pageSignin.signinButton)
        .expect(t.eval(() => window.location.pathname)).eql('/configuration/overview')
        .expect(userMenu.button.textContent).contains('John Doe');

    await t
        .wait(1000)
        .expect(t.ctx.touched).eql(true, 'Check that web socket created');
});
