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

import {Component, Inject, Output, EventEmitter, OnInit} from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import IgniteAdminData from '../../../app/core/admin/Admin.data';
import MessagesFactory from '../../../app/services/Messages.service';
import LoadingServiceFactory from '../../../app/modules/loading/loading.service';
import {customRequireValidator} from '../../common/validators';
import {SignupUserInfo} from 'app/modules/user/Auth.service';

@Component({
    selector: 'create-user',
    template: `
        <form [formGroup]="form" id="createUser" (ngSubmit)="createUser()" scrollToFirstInvalid>
            <form-signup [form]="form" [serverError]="serverError"></form-signup>
        </form>
    `
})
export class CreateUserComponent implements OnInit {

    static parameters = [
        [new Inject('$rootScope')],
        [new Inject('IgniteAdminData')],
        [new Inject('IgniteMessages')],
        [new Inject('IgniteLoading')],
        [new Inject(FormBuilder)]
    ];

    @Output() close: EventEmitter<void> = new EventEmitter();

    form: FormGroup;

    serverError: string | null = null;

    isLoading = false;

    constructor(
        protected $root: ng.IRootScopeService,
        protected AdminData: IgniteAdminData,
        protected IgniteMessages: ReturnType<typeof MessagesFactory>,
        protected loading: ReturnType<typeof LoadingServiceFactory>,
        protected fb: FormBuilder
    ) {}

    ngOnInit() {
        this.form = this.fb.group({
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

    createUser() {
        this.isLoading = true;

        this.form.markAsTouched();
        this.form.updateValueAndValidity();

        this.setServerError(null);

        if (!this.form.valid) return;

        this.loading.start('createUser');

        const data = this.getData();

        this.AdminData.registerUser(data)
        .then(() => {
            this.$root.$broadcast('userCreated');
            this.IgniteMessages.showInfo(`User ${data.email} created`);
            this.close.emit();
        })
        .catch((err) => {
            this.loading.finish('createUser');
            this.IgniteMessages.showError(null, err);
            this.setServerError(err.message);
        });
    }
}
