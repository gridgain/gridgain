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

import {Component, Inject} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import AuthService, {SignupUserInfo} from 'app/modules/user/Auth.service';
import MessagesFactory from 'app/services/Messages.service';
import {pipe, get, eq} from 'lodash/fp';
import {customRequireValidator} from '../../common/validators';

const EMAIL_NOT_CONFIRMED_ERROR_CODE = 10104;
const isEmailConfirmationError = pipe(get('data.errorCode'), eq(EMAIL_NOT_CONFIRMED_ERROR_CODE));

@Component({
    selector: 'page-signup',
    templateUrl: 'template.html',
    styleUrls: ['./style.url.scss']
})
export class PageSignupComponent {

    static parameters = [
        [new Inject('Auth')],
        [new Inject('IgniteMessages')],
        [new Inject(FormBuilder)]
    ];

    form: FormGroup;

    serverError: string | null = null;

    isLoading = false;

    constructor(
        private authService: AuthService,
        private IgniteMessages: ReturnType<typeof MessagesFactory>,
        private fb: FormBuilder
    ) {
        this.form = fb.group({
            email: ['', [Validators.required, Validators.email]],
            password: ['', customRequireValidator],
            confirm: ['', customRequireValidator],
            firstName: ['', customRequireValidator],
            lastName: ['', customRequireValidator],
            phone: [''],
            country: ['', customRequireValidator],
            company: ['', customRequireValidator]
        });
    }

    setServerError(error: any) {
        this.serverError = error;
    }

    getData(): SignupUserInfo {
        return {
            email: this.form.get('email').value.trim(),
            password: this.form.get('password').value.trim(),
            firstName: this.form.get('firstName').value.trim(),
            lastName: this.form.get('lastName').value.trim(),
            company: this.form.get('company').value.trim(),
            country: this.form.get('country').value.trim()
        };
    }

    signup() {
        this.isLoading = true;

        this.form.markAsTouched();
        this.form.updateValueAndValidity();

        this.setServerError(null);

        if (!this.form.valid) {
            this.isLoading = false;
            return;
        }

        return this.authService.signup(this.getData())
            .catch((err) => {
                if (isEmailConfirmationError(err))
                    return;

                this.IgniteMessages.showError(null, err.data);

                this.setServerError(err.data && err.data.message);
            })
            .finally(() => this.isLoading = false);
    }
}
