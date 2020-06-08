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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Rest client for tests
 */
public class TestRestClient {
    /** REST port. */
    public static final int DFLT_REST_PORT = 8091;

    /** Local host. */
    private static final String LOC_HOST = "127.0.0.1";

    /** JSON to java mapper. */
    private static final ObjectMapper JSON_MAPPER = new GridJettyObjectMapper();

    /** Signature provider. */
    private final Callable<String> signatureProvider;

    /**
     * Default constructor.
     */
    public TestRestClient() {
        signatureProvider = () -> null;
    }

    /**
     * @param provider Provider.
     */
    public TestRestClient(Callable<String> provider) {
        signatureProvider = provider;
    }

    /**
     * @return Port to use for rest. Needs to be changed over time because Jetty has some delay before port unbind.
     */
    public int restPort() {
        return DFLT_REST_PORT;
    }

    /**
     * @return Test URL
     */
    public String restUrl() {
        return "http://" + LOC_HOST + ":" + restPort() + "/ignite?";
    }

    /**
     * Execute REST command and return result.
     *
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    public String content(Map<String, String> params) throws Exception {
        SB sb = new SB(restUrl());

        for (Map.Entry<String, String> e : params.entrySet())
            sb.a(e.getKey()).a('=').a(e.getValue()).a('&');

        URL url = new URL(sb.toString());

        URLConnection conn = openConnection(url);

        InputStream in = conn.getInputStream();

        StringBuilder buf = new StringBuilder(256);

        try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, "UTF-8"))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine())
                buf.append(line);
        }

        return buf.toString();
    }

    /**
     * Open REST connection, set signature header if needed.
     *
     * @param url URL to open.
     * @return URL connection.
     * @throws Exception If failed.
     */
    public URLConnection openConnection(URL url) throws Exception {
        URLConnection conn = url.openConnection();

        String signature = signature();

        if (signature != null)
            conn.setRequestProperty("X-Signature", signature);

        return conn;
    }

    /**
     * @param cacheName Optional cache name.
     * @param cmd REST command.
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    public String content(String cacheName, GridRestCommand cmd, String... params) throws Exception {
        Map<String, String> paramsMap = new LinkedHashMap<>();

        if (cacheName != null)
            paramsMap.put("cacheName", cacheName);

        paramsMap.put("cmd", cmd.key());

        if (params != null) {
            assertEquals(0, params.length % 2);

            for (int i = 0; i < params.length; i += 2)
                paramsMap.put(params[i], params[i + 1]);
        }

        return content(paramsMap);
    }

    /**
     * @param json JSON content.
     * @param field Field name in JSON object.
     * @return Field value.
     * @throws IOException If failed.
     */
    public String jsonField(String json, String field) throws IOException {
        assertNotNull(json);
        assertFalse(json.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(json);

        JsonNode fld = node.get(field);

        assertNotNull(fld);

        return fld.asText();
    }

    /**
     * @return Signature
     */
    private String signature() throws Exception {
        return signatureProvider.call();
    }
}
