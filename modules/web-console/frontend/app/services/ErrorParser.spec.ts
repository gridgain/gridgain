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

import ErrorParser from './ErrorParser.service';
import JavaTypes from './JavaTypes.service';

const parser = new ErrorParser(new JavaTypes());

import { assert } from 'chai';

const FULL_ERROR_MSG = '[Exception1] Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause';

suite('Error parser service', () => {
    test('Error parsing', () => {
        assert.equal(FULL_ERROR_MSG, parser.extractFullMessage(TEST_CASES[0].error));

        TEST_CASES.forEach((testError) => {
            const parsed = parser.parse(testError.error, testError.prefix);

            assert.equal(testError.expectedMessage, parsed.message);

            assert.deepEqual(testError.expectedCauses, parsed.causes);
        });
    });
});

const TEST_CASES = [{
    expectedMessage: 'Test: Final cause',
    expectedCauses: ['Root cause (in lower case)', 'Duplicate cause', 'Final cause'],
    prefix: 'Test: ',
    error: {
        className: 'test.Exception1',
        message: `Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause, trace=...]
    at test.Class1.function(Class1.java:1)
    ... 9 more 
Caused by: class test.Exception2: Final cause
    at test.Class2.function(Class2.java:1)
    ... 14 more
Caused by: class test.Exception3: Duplicate cause
    at test.Class3.function(Class3.java:1)
    ... 14 more
Caused by: class test.Exception4: Duplicate cause
    at test.Class4.function(Class4.java:1)
    ... 29 more
Caused by: class test.Exception5: root cause (in lower case)
    at test.Class5.function(Class5.java:1)
    ... 40 more
`
    }
}, {
    expectedMessage: 'Final cause',
    expectedCauses: ['Final cause', 'First cause'],
    error: {
        className: '',
        message: `Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause, trace=...]
    at test.Class1.function(Class1.java:1)
    ... 9 more 
Caused by: class test.Exception2: first cause
    at test.Class2.function(Class2.java:1)
    ... 14 more
Caused by: class test.Exception3: Final cause
    at test.Class3.function(Class3.java:1)
    ... 14 more`
    }
}, {
    expectedMessage: 'Final cause',
    expectedCauses: ['First cause'],
    error: {
        className: 'test.Exception1',
        message: `Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause, trace=...]
    at test.Class1.function(Class1.java:1)
    ... 9 more 
Caused by: class test.Exception2: first cause
    at test.Class2.function(Class2.java:1)
    ... 14 more
Caused by: class test.Exception3: First cause
    at test.Class3.function(Class3.java:1)
    ... 14 more`
    }
}, {
    expectedMessage: 'Final cause',
    expectedCauses: ['First cause'],
    error: {
        className: 'test.Exception1',
        message: `Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause, trace=...]
    at test.Class1.function(Class1.java:1)
    ... 9 more 
Caused by: class test.Exception2: First cause
    at test.Class2.function(Class2.java:1)
    ... 14 more
Caused by: class test.Exception3: first cause
    at test.Class3.function(Class3.java:1)
    ... 14 more
Caused by: class test.Exception4: first cause
    at test.Class4.function(Class4.java:1)
    ... 29 more
Caused by: class test.Exception5: First cause
    at test.Class5.function(Class5.java:1)
    ... 40 more`
    }
}, {
    expectedMessage: 'Final cause',
    expectedCauses: ['Final cause', 'Duplicate cause', 'First cause'],
    error: {
        className: 'test.Exception1',
        message: `Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause, trace=...]
    at test.Class1.function(Class1.java:1)
    ... 9 more 
Caused by: class test.Exception2: First cause
    at test.Class2.function(Class2.java:1)
    ... 14 more
Caused by: class test.Exception3: Duplicate cause
    at test.Class3.function(Class3.java:1)
    ... 14 more
Caused by: class test.Exception4: Duplicate cause
    at test.Class4.function(Class4.java:1)
    ... 29 more
Caused by: class test.Exception5: final cause
    at test.Class5.function(Class5.java:1)
    ... 40 more`
    }
}];
