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
import org.hibernate.cache.cfg.spi.CollectionDataCachingConfig;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.cfg.spi.EntityDataCachingConfig;
import org.hibernate.cache.cfg.spi.NaturalIdDataCachingConfig;
import org.hibernate.cache.spi.CacheKeysFactory;
import org.hibernate.cache.spi.ExtendedStatisticsSupport;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.cache.spi.support.AbstractDomainDataRegion;

/** */
public class IgniteDomainDataRegion extends AbstractDomainDataRegion implements ExtendedStatisticsSupport {
    /** */
    private final HibernateCacheProxy cache;

    /** */
    private HibernateAccessStrategyFactory stgyFactory;

    /** */
    public IgniteDomainDataRegion(DomainDataRegionConfig regionCfg,
        RegionFactory regionFactory,
        CacheKeysFactory defKeysFactory,
        DomainDataRegionBuildingContext buildingCtx,
        HibernateAccessStrategyFactory stgyFactory) {
        super(regionCfg, regionFactory, defKeysFactory, buildingCtx);

        this.stgyFactory = stgyFactory;

        cache = stgyFactory.regionCache(getName());

        completeInstantiation(regionCfg, buildingCtx);
    }

    /** */
    @Override protected EntityDataAccess generateEntityAccess(EntityDataCachingConfig entityAccessCfg) {
        AccessType accessType = entityAccessCfg.getAccessType();
        Ignite ignite = stgyFactory.node();
        switch (accessType) {
            case READ_ONLY:
                HibernateAccessStrategyAdapter readOnlyStgy =
                    stgyFactory.createReadOnlyStrategy(cache);
                return new IgniteEntityDataAccess(readOnlyStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case NONSTRICT_READ_WRITE:
                HibernateAccessStrategyAdapter nonStrictReadWriteStgy =
                    stgyFactory.createNonStrictReadWriteStrategy(cache);
                return new IgniteEntityDataAccess(nonStrictReadWriteStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case READ_WRITE:
                HibernateAccessStrategyAdapter readWriteStgy =
                    stgyFactory.createReadWriteStrategy(cache);
                return new IgniteEntityDataAccess(readWriteStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case TRANSACTIONAL:
                HibernateAccessStrategyAdapter transactionalStgy =
                    stgyFactory.createTransactionalStrategy(cache);
                return new IgniteEntityDataAccess(transactionalStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }

    /** */
    @Override protected CollectionDataAccess generateCollectionAccess(CollectionDataCachingConfig cachingCfg) {
        HibernateCacheProxy cache = stgyFactory.regionCache(getName());
        AccessType accessType = cachingCfg.getAccessType();
        Ignite ignite = stgyFactory.node();
        switch (accessType) {
            case READ_ONLY:
                HibernateAccessStrategyAdapter readOnlyStgy =
                    stgyFactory.createReadOnlyStrategy(cache);
                return new IgniteCollectionDataAccess(readOnlyStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case NONSTRICT_READ_WRITE:
                HibernateAccessStrategyAdapter nonStrictReadWriteStgy =
                    stgyFactory.createNonStrictReadWriteStrategy(cache);
                return new IgniteCollectionDataAccess(nonStrictReadWriteStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case READ_WRITE:
                HibernateAccessStrategyAdapter readWriteStgy =
                    stgyFactory.createReadWriteStrategy(cache);
                return new IgniteCollectionDataAccess(readWriteStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case TRANSACTIONAL:
                HibernateAccessStrategyAdapter transactionalStgy =
                    stgyFactory.createTransactionalStrategy(cache);
                return new IgniteCollectionDataAccess(transactionalStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }

    /** */
    @Override protected NaturalIdDataAccess generateNaturalIdAccess(NaturalIdDataCachingConfig naturalIdAccessCfg) {
        HibernateCacheProxy cache = stgyFactory.regionCache(getName());
        AccessType accessType = naturalIdAccessCfg.getAccessType();
        Ignite ignite = stgyFactory.node();
        switch (accessType) {
            case READ_ONLY:
                HibernateAccessStrategyAdapter readOnlyStgy =
                    stgyFactory.createReadOnlyStrategy(cache);
                return new IgniteNaturalIdDataAccess(readOnlyStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case NONSTRICT_READ_WRITE:
                HibernateAccessStrategyAdapter nonStrictReadWriteStgy =
                    stgyFactory.createNonStrictReadWriteStrategy(cache);
                return new IgniteNaturalIdDataAccess(nonStrictReadWriteStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case READ_WRITE:
                HibernateAccessStrategyAdapter readWriteStgy =
                    stgyFactory.createReadWriteStrategy(cache);
                return new IgniteNaturalIdDataAccess(readWriteStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            case TRANSACTIONAL:
                HibernateAccessStrategyAdapter transactionalStgy =
                    stgyFactory.createTransactionalStrategy(cache);
                return new IgniteNaturalIdDataAccess(transactionalStgy, accessType, getRegionFactory(),
                    this, ignite, cache);

            default:
                throw new IllegalArgumentException("Unknown Hibernate access type: " + accessType);
        }
    }

    /** */
    @Override public void destroy() throws CacheException {
        // no-op
    }

    /** */
    @Override public long getElementCountInMemory() {
        return cache.offHeapEntriesCount();
    }

    /** */
    @Override public long getElementCountOnDisk() {
        return cache.sizeLong();
    }

    /** */
    @Override public long getSizeInMemory() {
        return cache.offHeapAllocatedSize();
    }
}
