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

package org.apache.ignite.console.web.security;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.session.ExpiringSession;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.console.common.Utils.getPrincipal;

/**
 * A {@link SessionRepository} backed by a Apache Ignite and that uses a {@link MapSession}.
 */
public class IgniteSessionRepository implements FindByIndexNameSessionRepository<ExpiringSession> {
    /** This value is used to override {@link ExpiringSession#setMaxInactiveIntervalInSeconds(int)}. */
    private int maxInactiveInterval;

    /** */
    private final TransactionManager txMgr;

    /** Sessions collection. */
    private CacheHolder<String, MapSession> sessionsCache;

    /** Account to sessions index. */
    private OneToManyIndex<String, String> accToSesIdx;

    /**
     * @param ignite Ignite.
     */
    public IgniteSessionRepository(long expirationTimeout, Ignite ignite, TransactionManager txMgr) {
       this.maxInactiveInterval = (int)MILLISECONDS.toSeconds(expirationTimeout);
       this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            sessionsCache = new CacheHolder<>(ignite, "wc_sessions", expirationTimeout);
            accToSesIdx = new OneToManyIndex<>(ignite, "wc_acc_to_ses_idx", expirationTimeout);
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
        txMgr.doInTransaction(() -> {
            Account acc = getPrincipal(ses);

            String sesId = ses.getId();

            if (acc != null && ses.getAttribute(PRINCIPAL_NAME_INDEX_NAME) == null) {
                String email = acc.getEmail();

                ses.setAttribute(PRINCIPAL_NAME_INDEX_NAME, email);

                accToSesIdx.add(email, sesId);
            }

            sessionsCache.put(ses.getId(), new MapSession(ses));
        });
    }

    /** {@inheritDoc} */
    @Override public ExpiringSession getSession(String id) {
        return txMgr.doInTransaction(() -> {
            ExpiringSession ses = sessionsCache.get(id);

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
    @Override public void delete(String sesId) {
        txMgr.doInTransaction(() -> {
            MapSession ses = sessionsCache.cache().getAndRemove(sesId);

            if (ses != null) {
                String email = ses.getAttribute(PRINCIPAL_NAME_INDEX_NAME);

                accToSesIdx.delete(email);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Map<String, ExpiringSession> findByIndexNameAndIndexValue(String idxName, String idxVal) {
        if (!PRINCIPAL_NAME_INDEX_NAME.equals(idxName))
            return Collections.emptyMap();

        return accToSesIdx
            .get(idxVal)
            .stream()
            .map(this::getSession)
            .filter(Objects::nonNull)
            .collect(toMap(Session::getId, identity()));
    }
}
