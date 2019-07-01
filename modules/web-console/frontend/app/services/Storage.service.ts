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

/**
 * Set value in local storage.
 *
 * @param {string} key Key of value to store.
 * @param {string} data Value to store.
 */
export function setLocalUnsafe(key: string, data: string) {
    localStorage.setItem(key, data);
}

/**
 * Set value in local storage. Any exceptions will be ignored.
 *
 * @param {string} key Key of value to store.
 * @param {string} data Value to store.
 */
export function setLocal(key: string, data: string) {
    try {
        setLocalUnsafe(key, data);
    }
    catch (ignore) {
        /** No-op. */
    }
}

/**
 * Get value from local storage.
 *
 * @param {string} key Key of value to read.
 * @param {string} dflt Default value in case if storage does not contain value with specified key.
 * @return {string} Value from store or default value if storage does not contain value with specified key.
 */
export function getLocalUnsafe(key: string, dflt: string = null) {
    return localStorage.getItem(key) || dflt;
}

/**
 * Get value from local storage. Any exceptions will be ignored.
 *
 * @param {string} key Key of value to read.
 * @param {string} dflt Default value in case if storage does not contain value with specified key.
 * @return {string} Value from store or default value if storage does not contain value with specified key.
 */
export function getLocal(key: string, dflt: string = null) {
    try {
        return getLocalUnsafe(key, dflt);
    }
    catch (ignore) {
        /** No-op. */

        return null;
    }
}

/**
 * Remove value with specified key from storage.
 *
 * @param {string} key Key of value to remove.
 */
export function removeLocal(key: string) {
    try {
        localStorage.removeItem(key);
    }
    catch (ignore) {
        /** No-op. */
    }
}
