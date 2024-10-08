/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.springdata.compoundkey;

import java.util.Objects;

/** Extended city compound key. */
public class CityKeyExt extends CityKey {

    /** City extended identifier. */
    private long idExt;

    /**
     * @param id city identifier
     * @param countryCode city countrycode
     * @param idExt city extended identifier
     * */
    public CityKeyExt(int id, String countryCode, long idExt) {
        super(id, countryCode);
        this.idExt = idExt;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        if (!super.equals(o))
            return false;

        CityKeyExt that = (CityKeyExt) o;

        return idExt == that.idExt;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), idExt);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return idExt + "|" + super.toString();
    }
}
