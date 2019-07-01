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

package org.apache.ignite.console.errors;

import javax.cache.CacheException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;

/**
 * Class with error codes and messages.
 */
public class Errors {
    /** */
    public static final String ERR_SIGNUP_NOT_ALLOWED = "err.sign-up-not-allowed";

    /** */
    public static final String ERR_CONFIRM_EMAIL = "err.confirm-email";

    /** */
    public static final String ERR_ACTIVATION_NOT_ENABLED = "err.activation-not-enabled";

    /** */
    public static final String ERR_TOO_MANY_ACTIVATION_ATTEMPTS = "err.too-many-activation-attempts";

    /** */
    public static final String ERR_DB_LOST_CONNECTION_DURING_TX = "err.db-lost-connection-during-tx";

    /** */
    public static final String ERR_ACTIVE_TX_NOT_FOUND = "err.active-tx-not-found";

    /** */
    public static final String ERR_DATA_ACCESS_VIOLATION = "err.data-access-violation";

    /** */
    public static final String ERR_DB_NOT_AVAILABLE = "err.db-not-available";

    /** */
    public static final String ERR_PROHIBITED_REVOKE_ADMIN_RIGHTS = "err.prohibited-revoke-admin-rights";

    /** */
    public static final String ERR_PARSE_SIGNIN_REQ_FAILED = "err.parse-signin-req-failed";

    /** */
    public static final String ERR_COULD_NOT_CREATE_HASH = "err.could-not-create-hash";

    /** */
    public static final String ERR_TOKENS_NO_SPECIFIED_IN_AGENT_HANDSHAKE_REQ = "err.tokens-no-specified-in-agent-handshake-req";

    /** */
    public static final String ERR_FAILED_AUTH_WITH_TOKENS = "err.failed-auth-with-tokens";

    /** */
    public static final String ERR_UNKNOWN_EVT = "err.unknown-evt";

    /** */
    public static final String ERR_NOT_SPECIFIED_TASK_ID = "err.not-specified-task-id";

    /** */
    public static final String ERR_UNKNOWN_TASK = "err.unknown-task";

    /** */
    public static final String ERR_AGENT_NOT_FOUND_BY_ACC_ID = "err.agent-not-found-by-acc-id";

    /** */
    public static final String ERR_AGENT_NOT_FOUND_BY_ACC_ID_AND_CLUSTER_ID = "err.agent-not-found-by-acc-id-and-cluster-id";

    /** */
    public static final String ERR_AGENT_UNSUPPORT_VERSION = "err.agent-unsupport-version";

    /** */
    public static final String ERR_AGENT_DIST_NOT_FOUND = "err.agent-dist-not-found";

    /** */
    public static final String ERR_CLUSTER_NOT_FOUND_BY_ID = "err.cluster-not-found-by-id";

    /** */
    public static final String ERR_CLUSTER_ID_NOT_FOUND = "err.cluster-id-not-found";

    /** */
    public static final String ERR_CLUSTER_NAME_IS_EMPTY = "err.cluster-name-is-empty";

    /** */
    public static final String ERR_CLUSTER_DISCOVERY_NOT_FOUND = "err.cluster-discovery-not-found";

    /** */
    public static final String ERR_CLUSTER_DISCOVERY_KIND_NOT_FOUND = "err.cluster-discovery-kind-not-found";

    /** */
    public static final String ERR_MISSING_CLUSTER_ID_PARAM = "err.missing-cluster-id-param";

    /** */
    public static final String ERR_MODEL_NOT_FOUND_BY_ID = "err.model-not-found-by-id";

    /** */
    public static final String ERR_MODEL_ID_NOT_FOUND = "err.model-id-not-found";

    /** */
    public static final String ERR_CACHE_NOT_FOUND_BY_ID = "err.cache-not-found-by-id";

    /** */
    public static final String ERR_CACHE_ID_NOT_FOUND = "err.cache-id-not-found";

    /** */
    public static final String ERR_ACCOUNT_NOT_FOUND_BY_TOKEN = "err.account-not-found-by-token";

    /** */
    public static final String ERR_ACCOUNT_NOT_FOUND_BY_ID = "err.account-not-found-by-id";

    /** */
    public static final String ERR_ACCOUNT_WITH_EMAIL_EXISTS = "err.account-with-email-exists";

    /** */
    public static final String ERR_ACCOUNT_WITH_TOKEN_EXISTS = "err.account-with-token-exists";

    /** */
    public static final String ERR_ACCOUNT_CANT_BE_FOUND_IN_WS_SESSION = "err.account-cant-be-found-in-ws-session";

    /** */
    public static final int ERR_EMAIL_NOT_CONFIRMED = 10104;

    /**
     * @param e Exception to check.
     * @return {@code true} if database not available.
     */
    public static boolean checkDatabaseNotAvailable(Throwable e) {
        if (e instanceof IgniteClientDisconnectedException)
            return true;

        if (e instanceof CacheException && e.getCause() instanceof IgniteClientDisconnectedException)
            return true;

        // TODO GG-19681: In future versions specific exception will be added.
        String msg = e.getMessage();

        return e instanceof IgniteException &&
            msg != null &&
            msg.startsWith("Cannot start/stop cache within lock or transaction");
    }


    /**
     * @param e Exception.
     * @param msg Message.
     * @return Exception to throw.
     */
    public static RuntimeException convertToDatabaseNotAvailableException(RuntimeException e, String msg) {
        return checkDatabaseNotAvailable(e) ? new DatabaseNotAvailableException(msg) : e;
    }
}
