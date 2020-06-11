/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.springdata.misc;

import java.io.Serializable;

/**
 * Compound key.
 */
public class PersonKey implements Serializable {
    /** */
    private int id1;

    /** */
    private int id2;

    /**
     * @param id1 ID1.
     * @param id2 ID2.
     */
    public PersonKey(int id1, int id2) {
        this.id1 = id1;
        this.id2 = id2;
    }

    /**
     * @return ID1
     */
    public int getId1() {
        return id1;
    }

    /**
     * @return ID1
     */
    public int getId2() {
        return id1;
    }
}
