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

import {dropTestDB, resolveUrl, insertTestUser} from '../../environment/envtools';
import {pageSignup as page} from '../../page-models/pageSignup';
import {errorNotification} from '../../components/notifications';
import {userMenu} from '../../components/userMenu';

const existEmail = 'exist.singup@exmaple.com';
const newEmail = 'new.singup@exmaple.com';

fixture('Signup')
    .page(resolveUrl('/signup'))
    .before(async() => {
        await dropTestDB(existEmail);
        await dropTestDB(newEmail);
        await insertTestUser(existEmail);
    })
    .after(async() => {
        await dropTestDB(existEmail);
        await dropTestDB(newEmail);
    });

test('Local validation', async(t) => {
    await page.fillSignupForm({
        email: 'foobar',
        password: '1',
        passwordConfirm: '2',
        firstName: '  ',
        lastName: 'Doe',
        company: 'FooBar',
        country: 'Brazil'
    });
    await t
        .expect(page.email.getError('email').exists).ok()
        .expect(page.passwordConfirm.getError('mismatch').exists).ok()
        .expect(page.firstName.getError('required').exists).ok();
});
test('Server validation', async(t) => {
    await page.fillSignupForm({
        email: existEmail,
        password: '1',
        passwordConfirm: '1',
        firstName: 'John',
        lastName: 'Doe',
        company: 'FooBar',
        country: 'Brazil'
    });
    await t
        .click(page.signupButton)
        .expect(errorNotification.withText(`The email address you have entered is already registered: ${existEmail}`).exists).ok('Shows global error')
        .expect(page.email.getError('server').exists).ok('Marks email input as server-invalid');
});
test('Successful signup', async(t) => {
    await page.fillSignupForm({
        email: newEmail,
        password: '1',
        passwordConfirm: '1',
        firstName: 'John',
        lastName: 'Doe',
        company: 'FooBar',
        country: 'Brazil'
    });
    await t
        .click(page.signupButton)
        .expect(userMenu.button.textContent).contains('John Doe', 'User gets logged in under chosen full name');
});
