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

import { Component, Input, Inject, OnInit, OnChanges, OnDestroy, Output, EventEmitter } from '@angular/core';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import CountriesService from 'app/services/Countries.service';
import { FORM_FIELD_OPTIONS, FormFieldRequiredMarkerStyles, FormFieldErrorStyles, passwordMatch } from '../form-field';
import { tap, takeUntil } from 'rxjs/operators';
import { Subject } from 'rxjs';

@Component({
    selector: 'form-signup',
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
export class FormSignupComponent implements OnInit, OnChanges, OnDestroy {

    static parameters = [
        [new Inject(FormBuilder)],
        [new Inject('IgniteCountries')]
    ];

    @Input() form: FormGroup;
    @Input() serverError: string | null;

    countries: any[];

    onDestroy$: Subject<any> = new Subject();

    constructor(
        private fb: FormBuilder,
        private Countries: ReturnType<typeof CountriesService>
    ) {
        this.countries = Countries.getAll();
        this.serverValidator = this.serverValidator.bind(this);
    }

    ngOnChanges(changes: any) {
        if (changes.serverError && this.form) {
            this.form.get('email').setValidators([Validators.required, Validators.email, this.serverValidator]);
            this.form.get('email').updateValueAndValidity();
        }
    }

    ngOnInit() {
        this.form.get('password').valueChanges.pipe(
            takeUntil(this.onDestroy$),
            tap((newPassword: string) => {
                this.form.get('confirm').setValidators([passwordMatch(newPassword), Validators.required]);
                this.form.get('confirm').updateValueAndValidity();
            })
        ).subscribe();
    }

    ngOnDestroy() {
        this.onDestroy$.next();
        this.onDestroy$.complete();
    }

    serverValidator() {
        return this.serverError ? {server: true} : null;
    }
}
