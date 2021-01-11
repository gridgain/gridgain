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

package org.apache.ignite.platform.model;

import java.util.Objects;

/** */
public class Account {
    /** */
    private String id;

    /** */
    private int amount;

    /** */
    public Account() {
    }

    public Account(String id, int amount) {
        this.id = id;
        this.amount = amount;
    }

    /** */
    public String getId() {
        return id;
    }

    /** */
    public void setId(String id) {
        this.id = id;
    }

    /** */
    public int getAmount() {
        return amount;
    }

    /** */
    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Account account = (Account)o;
        return Objects.equals(id, account.id);
    }

    @Override public int hashCode() {
        return Objects.hash(id);
    }
}
