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

const path = require('path');
const fireUp = require('fire-up');

module.exports = fireUp.newInjector({
    basePath: path.join(__dirname, '../'),
    modules: [
        './app/**/*.js',
        './config/**/*.js',
        './errors/**/*.js',
        './middlewares/**/*.js',
        './routes/**/*.js',
        './services/**/*.js',
        './test/app/*.js'
    ],
    use: [
        'settings:mock',
        'services/mails:mock'
    ]
});
