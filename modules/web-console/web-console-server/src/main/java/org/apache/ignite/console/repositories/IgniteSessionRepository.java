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

package org.apache.ignite.console.repositories;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.stereotype.Repository;

/**
 * A {@link SessionRepository} backed by a Apache Ignite and that uses a {@link MapSession}.
 */
@Repository
public class IgniteSessionRepository implements SessionRepository<ExpiringSession> {
    /** The number of seconds that the {@link Session} should be kept alive between requests (default: 30 days). */
    @Value("${server.sessions.inactive.timeout:2592000}")
    private int maxInactiveInterval;

    /** */
    protected final TransactionManager txMgr;

    /** Session cache. */
    private IgniteCache<String, MapSession> cache;

    /**
     * @param ignite Ignite.
     * @param txMgr Transaction manager.
     */
    public IgniteSessionRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            CacheConfiguration<String, MapSession> cfg = new CacheConfiguration<String, MapSession>()
                .setName("wc_sessions")
                .setCacheMode(CacheMode.REPLICATED);

            cache = ignite.getOrCreateCache(cfg);
        });
    }

    /** {@inheritDoc} */
    @Override public ExpiringSession createSession() {
        ExpiringSession ses = new MapSession();

        ses.setMaxInactiveIntervalInSeconds(maxInactiveInterval);

        return ses;
    }

    /** {@inheritDoc} */
    @Override public void save(ExpiringSession ses) {
        txMgr.doInTransaction(() -> cache.put(ses.getId(), new MapSession(ses)));
    }

    /** {@inheritDoc} */
    @Override public ExpiringSession getSession(String id) {
        return txMgr.doInTransaction(() -> {
            ExpiringSession ses = cache.get(id);

            if (ses == null)
                return null;

            if (ses.isExpired()) {
                delete(ses.getId());

                return null;
            }

            return ses;
        });
    }

    /** {@inheritDoc} */
    @Override public void delete(String id) {
        txMgr.doInTransaction(() -> cache.remove(id));
    }
}
