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
import Auth from 'app/modules/user/Auth.service';
import MessagesFactory from 'app/services/Messages.service';

export const getParamMapForCurrentLocation = (): Map<string, string> => {
    const paramMap = new Map();
    const search = window.location.search.slice(1).split('&').map((s) => s.split('='));
    search.forEach((s) => paramMap.set(s[0], s[1]));
    return paramMap;
};

@Component({
    selector: 'page-signup-confirmation',
    template: `
    <h3 class="public-page__title">Confirm your email</h3>
    <p>
        Thanks For Signing Up!
        <br>
        Please check your email and click link in the message we just sent to <b>{{email}}</b>
        <br>
        If you don’t receive email try to <a (click)='resendConfirmation()'>resend confirmation</a> once more.
    </p>
    `,
    styleUrls: ['./style.url.scss']
})

export class PageSignupConfirmationComponent implements OnInit {

    static parameters = [
        [new Inject('Auth')],
        [new Inject('IgniteMessages')]
    ]

    email: string;

    constructor(private auth: Auth, private messages: ReturnType<typeof MessagesFactory>) {}

    ngOnInit() {
        // @todo Change to ActivatedRoute
        this.email = getParamMapForCurrentLocation().get('email');
    }

    async resendConfirmation() {
        try {
            await this.auth.resendSignupConfirmation(this.email);
            this.messages.showInfo('Signup confirmation sent, check your email');
        }
        catch (e) {
            this.messages.showError(e);
        }
    }
}
