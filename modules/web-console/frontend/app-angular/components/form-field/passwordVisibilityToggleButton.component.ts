/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Component, Input, ElementRef} from '@angular/core';

@Component({
    selector: 'password-visibility-toggle-button-angular',
    template: `
        <button
            type='button'
            (click)='toggleVisibility()'
            [popper]='isVisible ? "Hide password" : "Show password"'
            popperApplyClass='ignite-popper,ignite-popper__tooltip'
            popperAppendTo='body'
            popperTrigger='hover'
            popperPlacement='top'
        >
            <ignite-icon [name]='isVisible ? "eyeOpened" : "eyeClosed"'></ignite-icon>
        </button>
    `,
    styles: [`
        :host {
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }
        button {
            border: none;
            margin: 0;
            padding: 0;
            background: none;
            display: inline-flex;
        }
        button:hover, button:active {
            color: #0067b9;
        }
    `]
})
export class PasswordVisibilityToggleButton {
    @Input()
    passwordEl: HTMLInputElement
    isVisible: boolean = false
    toggleVisibility() {
        this.isVisible = !this.isVisible;
        this.passwordEl.setAttribute('type', this.isVisible ? 'text' : 'password');
    }
}

