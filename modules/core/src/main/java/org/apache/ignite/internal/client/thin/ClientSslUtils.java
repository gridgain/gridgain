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

package org.apache.ignite.internal.client.thin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.configuration.ClientConfiguration;

import static org.apache.ignite.ssl.SslContextFactory.DFLT_KEY_ALGORITHM;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;

public class ClientSslUtils {
    /** */
    public static final char[] EMPTY_CHARS = new char[0];

    /** Trust manager ignoring all certificate checks. */
    private static final TrustManager ignoreErrorsTrustMgr = new X509TrustManager() {
        /** */
        @Override public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        /** */
        @Override public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
            // No-op.
        }

        /** */
        @Override public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
            // No-op.
        }
    };

    /**
     * Gets SSL context for the given client configuration.
     *
     * @param cfg Configuration.
     * @return {@link SSLContext} when SSL is enabled in the configuration; null otherwise.
     */
    public static SSLContext getSslContext(ClientConfiguration cfg) {
        if (cfg.getSslMode() == SslMode.DISABLED)
            return null;

        Factory<SSLContext> sslCtxFactory = cfg.getSslContextFactory();

        if (sslCtxFactory != null) {
            try {
                return sslCtxFactory.create();
            }
            catch (Exception e) {
                throw new ClientException("SSL Context Factory failed", e);
            }
        }

        BiFunction<String, String, String> or = (val, dflt) -> val == null || val.isEmpty() ? dflt : val;

        String keyStore = or.apply(
                cfg.getSslClientCertificateKeyStorePath(),
                System.getProperty("javax.net.ssl.keyStore")
        );

        String keyStoreType = or.apply(
                cfg.getSslClientCertificateKeyStoreType(),
                or.apply(System.getProperty("javax.net.ssl.keyStoreType"), DFLT_STORE_TYPE)
        );

        String keyStorePwd = or.apply(
                cfg.getSslClientCertificateKeyStorePassword(),
                System.getProperty("javax.net.ssl.keyStorePassword")
        );

        String trustStore = or.apply(
                cfg.getSslTrustCertificateKeyStorePath(),
                System.getProperty("javax.net.ssl.trustStore")
        );

        String trustStoreType = or.apply(
                cfg.getSslTrustCertificateKeyStoreType(),
                or.apply(System.getProperty("javax.net.ssl.trustStoreType"), DFLT_STORE_TYPE)
        );

        String trustStorePwd = or.apply(
                cfg.getSslTrustCertificateKeyStorePassword(),
                System.getProperty("javax.net.ssl.trustStorePassword")
        );

        String algorithm = or.apply(cfg.getSslKeyAlgorithm(), DFLT_KEY_ALGORITHM);

        String proto = toString(cfg.getSslProtocol());

        if (Stream.of(keyStore, keyStorePwd, trustStore, trustStorePwd)
                .allMatch(s -> s == null || s.isEmpty())
        ) {
            try {
                return SSLContext.getDefault();
            }
            catch (NoSuchAlgorithmException e) {
                throw new ClientException("Default SSL context cryptographic algorithm is not available", e);
            }
        }

        KeyManager[] keyManagers = getKeyManagers(algorithm, keyStore, keyStoreType, keyStorePwd);

        TrustManager[] trustManagers = cfg.isSslTrustAll() ?
                new TrustManager[] {ignoreErrorsTrustMgr} :
                getTrustManagers(algorithm, trustStore, trustStoreType, trustStorePwd);

        try {
            SSLContext sslCtx = SSLContext.getInstance(proto);

            sslCtx.init(keyManagers, trustManagers, null);

            return sslCtx;
        }
        catch (NoSuchAlgorithmException e) {
            throw new ClientException("SSL context cryptographic algorithm is not available", e);
        }
        catch (KeyManagementException e) {
            throw new ClientException("Failed to create SSL Context", e);
        }
    }

    /**
     * @return String representation of {@link SslProtocol} as required by {@link SSLContext}.
     */
    private static String toString(SslProtocol proto) {
        switch (proto) {
            case TLSv1_1:
                return "TLSv1.1";

            case TLSv1_2:
                return "TLSv1.2";

            case TLSv1_3:
                return "TLSv1.3";

            default:
                return proto.toString();
        }
    }

    /** */
    private static KeyManager[] getKeyManagers(
            String algorithm,
            String keyStore,
            String keyStoreType,
            String keyStorePwd
    ) {
        KeyManagerFactory keyMgrFactory;

        try {
            keyMgrFactory = KeyManagerFactory.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException e) {
            throw new ClientException("Key manager cryptographic algorithm is not available", e);
        }

        Predicate<String> empty = s -> s == null || s.isEmpty();

        if (!empty.test(keyStore) && !empty.test(keyStoreType)) {
            char[] pwd = (keyStorePwd == null) ? EMPTY_CHARS : keyStorePwd.toCharArray();

            KeyStore store = loadKeyStore("Client", keyStore, keyStoreType, pwd);

            try {
                keyMgrFactory.init(store, pwd);
            }
            catch (UnrecoverableKeyException e) {
                throw new ClientException("Could not recover key store key", e);
            }
            catch (KeyStoreException e) {
                throw new ClientException(
                        String.format("Client key store provider of type [%s] is not available", keyStoreType),
                        e
                );
            }
            catch (NoSuchAlgorithmException e) {
                throw new ClientException("Client key store integrity check algorithm is not available", e);
            }
        }

        return keyMgrFactory.getKeyManagers();
    }

    /** */
    private static TrustManager[] getTrustManagers(
            String algorithm,
            String trustStore,
            String trustStoreType,
            String trustStorePwd
    ) {
        TrustManagerFactory trustMgrFactory;

        try {
            trustMgrFactory = TrustManagerFactory.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException e) {
            throw new ClientException("Trust manager cryptographic algorithm is not available", e);
        }

        Predicate<String> empty = s -> s == null || s.isEmpty();

        if (!empty.test(trustStore) && !empty.test(trustStoreType)) {
            char[] pwd = (trustStorePwd == null) ? EMPTY_CHARS : trustStorePwd.toCharArray();

            KeyStore store = loadKeyStore("Trust", trustStore, trustStoreType, pwd);

            try {
                trustMgrFactory.init(store);
            }
            catch (KeyStoreException e) {
                throw new ClientException(
                        String.format("Trust key store provider of type [%s] is not available", trustStoreType),
                        e
                );
            }
        }

        return trustMgrFactory.getTrustManagers();
    }

    /** */
    private static KeyStore loadKeyStore(String lb, String path, String type, char[] pwd) {
        KeyStore store;

        try {
            store = KeyStore.getInstance(type);
        }
        catch (KeyStoreException e) {
            throw new ClientException(
                    String.format("%s key store provider of type [%s] is not available", lb, type),
                    e
            );
        }

        try (InputStream in = new FileInputStream(new File(path))) {

            store.load(in, pwd);

            return store;
        }
        catch (FileNotFoundException e) {
            throw new ClientException(String.format("%s key store file [%s] does not exist", lb, path), e);
        }
        catch (NoSuchAlgorithmException e) {
            throw new ClientException(
                    String.format("%s key store integrity check algorithm is not available", lb),
                    e
            );
        }
        catch (CertificateException e) {
            throw new ClientException(String.format("Could not load certificate from %s key store", lb), e);
        }
        catch (IOException e) {
            throw new ClientException(String.format("Could not read %s key store", lb), e);
        }
    }
}
