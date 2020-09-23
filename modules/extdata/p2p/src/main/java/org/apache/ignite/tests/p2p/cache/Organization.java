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

package org.apache.ignite.tests.p2p.cache;

/**
 */
public class Organization {
    /** Title. */
    String title;

    /** Head. */
    Person head;

    /** Address. */
    Address addr;

    public Organization(String title, Person head, Address addr) {
        this.title = title;
        this.head = head;
        this.addr = addr;
    }

    public String getTitle() {
        return title;
    }

    public Person getHead() {
        return head;
    }

    public Address getAddr() {
        return addr;
    }
}
