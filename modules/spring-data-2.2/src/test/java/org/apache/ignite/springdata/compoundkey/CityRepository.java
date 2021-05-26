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

package org.apache.ignite.springdata.compoundkey;

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.springdata22.repository.IgniteRepository;
import org.apache.ignite.springdata22.repository.config.RepositoryConfig;
import org.springframework.stereotype.Repository;

/** City repository */
@Repository
@RepositoryConfig(cacheName = "City", autoCreateCache = true)
public interface CityRepository extends IgniteRepository<City, CityKey> {
    /**
     * Find city by id
     * @param id city identifier
     * @return city
     * */
    public City findById(int id);

    /**
     * Find all cities by coutrycode
     * @param cc coutrycode
     * @return list of cache enrties CityKey -> City
     * */
    public List<Cache.Entry<CityKey, City>> findByCountryCode(String cc);
}
