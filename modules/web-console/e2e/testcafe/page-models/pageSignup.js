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

import {Selector, t} from 'testcafe';
import {CustomFormField} from '../components/FormField';
import { globalProgressIndicator } from '../components/globalProgressIndicator';

export const emailInput = new CustomFormField({id: 'emailInput'});
export const passwordInput = new CustomFormField({id: 'passwordInput'});
export const passwordConfirmInput = new CustomFormField({id: 'confirmInput'});
export const firstNameInput = new CustomFormField({id: 'firstNameInput'});
export const lastNameInput = new CustomFormField({id: 'lastNameInput'});
export const companyInput = new CustomFormField({id: 'companyInput'});
export const countryInput = new CustomFormField({id: 'countryInput'});
export const signupButton = Selector('button').withText('Sign Up');

export const progressIndicator = globalProgressIndicator;