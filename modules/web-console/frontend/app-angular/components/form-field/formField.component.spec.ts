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

import 'zone.js/dist/zone';

// https://stackoverflow.com/q/52176565/333777
import 'zone.js/dist/long-stack-trace-zone';
import 'zone.js/dist/proxy';
import 'zone.js/dist/sync-test';
import 'zone.js/dist/mocha-patch';
import 'zone.js/dist/async-test';
import 'zone.js/dist/fake-async-test';
import 'zone.js/dist/zone-patch-promise-test';

import 'core-js/es7/reflect';

import {
    BrowserDynamicTestingModule,
    platformBrowserDynamicTesting
} from '@angular/platform-browser-dynamic/testing';
import {ReactiveFormsModule, FormControl, Validators, FormGroup} from '@angular/forms';

import {assert} from 'chai';
import {TestBed, async, ComponentFixture, tick, fakeAsync} from '@angular/core/testing';
import {Component, Directive, NO_ERRORS_SCHEMA} from '@angular/core';
import {
    FormField, FormFieldError, FormFieldHint, FormFieldTooltip,
    FormFieldRequiredMarkerStyles
} from './index';
import {FormFieldErrors} from './errors.component';
import {IgniteIcon} from '../igniteIcon.component';

TestBed.initTestEnvironment(BrowserDynamicTestingModule, platformBrowserDynamicTesting());

suite.only('form-field', () => {
    let fixture: ComponentFixture<HostComponent>;
	@Component({
	    template: `
	    <div [formGroup]='form'>
			<form-field [requiredMarkerStyle]='markerStyle'>
				<label for="one">One:</label>
				<input type="text" id="one" formControlName='one'>
			</form-field>
	    </div>
		`
	})
    class HostComponent {
    	form = new FormGroup({
    		one: new FormControl(null, [Validators.required])
    	})
    	markerStyle = FormFieldRequiredMarkerStyles.REQUIRED
	}
    @Component({selector: 'popper-content', template: ''}) class PopperContentStub {}
    @Directive({selector: '[popper]'}) class PopperStub {}

    setup(fakeAsync(async() => {
	    TestBed.configureTestingModule({
	        declarations: [
		        FormField,
		        HostComponent
	        ],
	        schemas: [NO_ERRORS_SCHEMA],
	        imports: [ReactiveFormsModule]
	    }).compileComponents().then(() => {

		    fixture = TestBed.createComponent(HostComponent);
		    fixture.detectChanges();
		    tick();
		    fixture.detectChanges();
	    });
    }));
    test('Required host class', () => {
    	assert.ok(fixture.nativeElement.querySelector('form-field').matches('.form-field__required'));
    });
});
