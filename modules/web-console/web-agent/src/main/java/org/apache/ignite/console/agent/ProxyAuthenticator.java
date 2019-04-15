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

package org.apache.ignite.console.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.util.List;
import okhttp3.Authenticator;
import okhttp3.Challenge;
import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import static java.net.Authenticator.RequestorType.PROXY;

/**
 * Request interactive proxy credentials.
 *
 * Configure OkHttp to use {@link OkHttpClient.Builder#proxyAuthenticator(Authenticator)}.
 */
public class ProxyAuthenticator implements Authenticator {
    /** Latest credential hash code. */
    private int latestCredHashCode = 0;

    /** {@inheritDoc} */
    @Override public Request authenticate(Route route, Response res) throws IOException {
        List<Challenge> challenges = res.challenges();

        for (Challenge challenge : challenges) {
            if (!"Basic".equalsIgnoreCase(challenge.scheme()))
                continue;

            Request req = res.request();
            HttpUrl url = req.url();
            Proxy proxy = route.proxy();

            InetSocketAddress proxyAddr = (InetSocketAddress)proxy.address();

            PasswordAuthentication auth = java.net.Authenticator.requestPasswordAuthentication(
                proxyAddr.getHostName(), proxyAddr.getAddress(), proxyAddr.getPort(),
                url.scheme(), challenge.realm(), challenge.scheme(), url.url(), PROXY);

            if (auth != null) {
                String cred = Credentials.basic(auth.getUserName(), new String(auth.getPassword()), challenge.charset());

                if (latestCredHashCode == cred.hashCode()) {
                    latestCredHashCode = 0;

                    throw new ProxyAuthException("Failed to authenticate with proxy");
                }

                latestCredHashCode = cred.hashCode();

                return req.newBuilder()
                    .header("Proxy-Authorization", cred)
                    .build();
            }
        }

        return null; // No challenges were satisfied!
    }
}
