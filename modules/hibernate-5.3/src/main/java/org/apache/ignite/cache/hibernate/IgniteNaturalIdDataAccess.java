/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.hibernate;

import org.apache.ignite.Ignite;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;

/** */
public class IgniteNaturalIdDataAccess extends IgniteCachedDomainDataAccess implements NaturalIdDataAccess {
    /** */
    private final AccessType accessType;

    /** */
    public IgniteNaturalIdDataAccess(HibernateAccessStrategyAdapter stgy, AccessType accessType,
        RegionFactory regionFactory,
        DomainDataRegion domainDataRegion, Ignite ignite,
        HibernateCacheProxy cache) {
        super(stgy, regionFactory, domainDataRegion, ignite, cache);

        this.accessType = accessType;
    }

    /** */
    @Override public AccessType getAccessType() {
        return accessType;
    }

    /** */
    @Override public Object generateCacheKey(Object[] naturalIdValues, EntityPersister persister, SharedSessionContractImplementor ses) {
        return DefaultCacheKeysFactory.staticCreateNaturalIdKey(naturalIdValues, persister, ses);
    }

    /** */
    @Override public Object[] getNaturalIdValues(Object cacheKey) {
        return DefaultCacheKeysFactory.staticGetNaturalIdValues(cacheKey);
    }

    /** */
    @Override public boolean insert(SharedSessionContractImplementor ses, Object key, Object val) throws CacheException {
        return stgy.insert(key, val);
    }

    /** */
    @Override public boolean afterInsert(SharedSessionContractImplementor ses, Object key, Object val) throws CacheException {
        return stgy.afterInsert(key, val);
    }

    /** */
    @Override public boolean update(SharedSessionContractImplementor ses, Object key, Object val) throws CacheException {
        return stgy.update(key, val);
    }

    /** */
    @Override public boolean afterUpdate(SharedSessionContractImplementor ses, Object key, Object val, SoftLock lock) throws CacheException {
        return stgy.afterUpdate(key, val);
    }
}
