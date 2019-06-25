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

import {Component, OnInit, Inject} from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import Auth from 'app/modules/user/Auth.service';
import MessagesFactory from 'app/services/Messages.service';
import {FORM_FIELD_OPTIONS, FormFieldErrorStyles, FormFieldRequiredMarkerStyles} from '../form-field';

export const getParamMapForCurrentLocation = (): Map<string, string> => {
    const paramMap = new Map();
    const search = window.location.search.slice(1).split('&').map((s) => s.split('='));
    search.forEach((s) => paramMap.set(s[0], s[1]));
    return paramMap;
};

@Component({
    selector: 'page-signin',
    templateUrl: './template.html',
    styleUrls: ['./style.url.scss'],
    viewProviders: [
        {
            provide: FORM_FIELD_OPTIONS,
            useValue: {
                requiredMarkerStyle: FormFieldRequiredMarkerStyles.OPTIONAL,
                errorStyle: FormFieldErrorStyles.ICON
            }
        }
    ]
})

export class PageSigninComponent implements OnInit {

    static parameters = [
        [new Inject('Auth')],
        [new Inject('IgniteMessages')],
        [new Inject(FormBuilder)]
    ];

    activationToken?: string;

    form: FormGroup;

    serverError: string = null;

    isLoading = false;

    constructor(private Auth: Auth, private IgniteMessages: ReturnType<typeof MessagesFactory>, private fb: FormBuilder) {
        this.serverValidator = this.serverValidator.bind(this);
    }

    ngOnInit() {
        this.form = this.fb.group({
            email: ['', [Validators.required, Validators.email]],
            password: ['', [Validators.required]]
        });

        this.activationToken = getParamMapForCurrentLocation().get('activationToken');
    }

    setServerError(error: any) {
        this.serverError = error;
        this.form.controls.email.setValidators([Validators.required, Validators.email, this.serverValidator]);
        this.form.controls.email.updateValueAndValidity();
    }

    getData() {
        return {
            email: this.form.controls.email.value.trim(),
            password: this.form.controls.password.value.trim(),
            activationToken: this.activationToken || ''
        };
    }

    serverValidator() {
        return this.serverError ? {server: true} : null;
    }

    signin() {
        this.isLoading = true;

        this.form.markAsTouched();
        this.form.updateValueAndValidity();

        this.setServerError(null);

        if (!this.form.valid) {
            this.isLoading = false;
            return;
        }

        return this.Auth.signin(this.getData())
            .catch((err) => {
                this.IgniteMessages.showError(null, err.data);

                this.setServerError(err.data && err.data.message);

                this.isLoading = false;
            });
    }
}
