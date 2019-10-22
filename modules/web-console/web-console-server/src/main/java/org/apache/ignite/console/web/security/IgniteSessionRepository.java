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
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.tx.TransactionManager;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.session.ExpiringSession;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.console.common.Utils.getPrincipal;
import static org.apache.ignite.console.errors.Errors.convertToDatabaseNotAvailableException;

/**
 * A {@link SessionRepository} backed by a Apache Ignite and that uses a {@link MapSession}.
 */
public class IgniteSessionRepository implements FindByIndexNameSessionRepository<ExpiringSession> {
    /** Messages accessor. */
    private final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** If non-null, this value is used to override {@link ExpiringSession#setMaxInactiveIntervalInSeconds(int)}. */
    private Integer dfltMaxInactiveInterval;

    /** */
    private final TransactionManager txMgr;

    /** Sessions collection. */
    private CacheHolder<String, MapSession> sessionsCache;

    /** Account to sessions index. */
    private OneToManyIndex<String, String> accToSesIdx;

    /** Sessions to account index. */
    private OneToManyIndex<String, String> sesToAccIdx;

    /**
     * @param ignite Ignite.
     */
    public IgniteSessionRepository(Ignite ignite, TransactionManager txMgr) {
       this.txMgr = txMgr;

        txMgr.registerStarter(() -> {
            sessionsCache = new CacheHolder<>(ignite, "wc_sessions");
            accToSesIdx = new OneToManyIndex<>(ignite, "wc_acc_to_ses_idx");
            sesToAccIdx = new OneToManyIndex<>(ignite, "wc_ses_to_acc_idx");
        });
    }

    /**
     * If non-null, this value is used to override {@link ExpiringSession#setMaxInactiveIntervalInSeconds(int)}.
     *
     * @param dfltMaxInactiveInterval Number of seconds that the {@link Session} should be kept alive between client
     * requests.
     */
    public IgniteSessionRepository setDefaultMaxInactiveInterval(int dfltMaxInactiveInterval) {
        this.dfltMaxInactiveInterval = dfltMaxInactiveInterval;

        return this;
    }

    /** {@inheritDoc} */
    @Override public ExpiringSession createSession() {
        ExpiringSession ses = new MapSession();

        if (dfltMaxInactiveInterval != null)
            ses.setMaxInactiveIntervalInSeconds(dfltMaxInactiveInterval);

        return ses;
    }

    /** {@inheritDoc} */
    @Override public void save(ExpiringSession ses) {
        try {
            txMgr.doInTransaction(() -> {
                Account acc = getPrincipal(ses);

                String sesId = ses.getId();

                if (acc != null) {
                    String email = acc.getEmail();

                    ses.setAttribute(PRINCIPAL_NAME_INDEX_NAME, acc.getEmail());

                    accToSesIdx.add(email, sesId);
                    sesToAccIdx.add(sesId, email);
                }

                sessionsCache.cache().put(ses.getId(), new MapSession(ses));
            });
        }
        catch (RuntimeException e) {
            throw convertToDatabaseNotAvailableException(e, messages.getMessage("err.db-not-available"));
        }
    }

    /** {@inheritDoc} */
    @Override public ExpiringSession getSession(String id) {
        try {
            ExpiringSession ses = sessionsCache.get(id);

            if (ses == null)
                return null;

            if (ses.isExpired()) {
                delete(ses.getId());

                return null;
            }

            return ses;
        }
        catch (RuntimeException e) {
            throw convertToDatabaseNotAvailableException(e, messages.getMessage("err.db-not-available"));
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(String sesId) {
        try {
            txMgr.doInTransaction(() -> {
                Set<String> accEmails = sesToAccIdx.get(sesId);

                sesToAccIdx.delete(sesId);

                accEmails.forEach(accToSesIdx::delete);

                sessionsCache.cache().remove(sesId);
            });
        }
        catch (RuntimeException e) {
            throw convertToDatabaseNotAvailableException(e, messages.getMessage("err.db-not-available"));
        }
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
