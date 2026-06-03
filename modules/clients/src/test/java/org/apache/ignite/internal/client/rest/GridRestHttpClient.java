/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/** Issues a request against the node's Jetty REST endpoint and parses the JSON body. */
public final class GridRestHttpClient {
    /** */
    private GridRestHttpClient() {
        // No-op.
    }

    /**
     * @param port Jetty REST port.
     * @param pathQ Path with query string.
     * @return HTTP status code and parsed body.
     * @throws IOException If the request failed.
     */
    public static Response get(int port, String pathQ) throws IOException {
        return http(port, "GET", pathQ);
    }

    /**
     * Body-bearing methods ({@code POST}/{@code PUT}) send an explicit empty entity.
     *
     * @param port Jetty REST port.
     * @param method HTTP method.
     * @param pathQ Path with query string.
     * @return HTTP status code and parsed body.
     * @throws IOException If the request failed.
     */
    public static Response http(int port, String method, String pathQ) throws IOException {
        URL url = new URL("http://localhost:" + port + pathQ);

        HttpURLConnection conn = (HttpURLConnection)url.openConnection();

        conn.setRequestMethod(method);

        // Bound waits so a hung request fails the test instead of stalling.
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(15_000);

        // Body-bearing methods: send an explicit empty entity so the server doesn't wait on a
        // Content-Length it never receives.
        if ("POST".equals(method) || "PUT".equals(method)) {
            conn.setDoOutput(true);
            conn.setFixedLengthStreamingMode(0);
        }

        conn.connect();

        if (conn.getDoOutput())
            conn.getOutputStream().close();

        int code = conn.getResponseCode();
        boolean isHTTP_OK = code == HttpURLConnection.HTTP_OK;

        Map<String, Object> body;

        try (InputStreamReader streamReader = new InputStreamReader(isHTTP_OK ? conn.getInputStream() : conn.getErrorStream())) {
            body = new ObjectMapper().readValue(streamReader, new TypeReference<Map<String, Object>>() { /* No-op. */ });
        }
        catch (IOException e) {
            throw new IOException("Failed to read REST response [url=" + url + ", code=" + code + "]", e);
        }

        return new Response(code, body);
    }

    /** HTTP status code and parsed JSON body. */
    public static final class Response {
        /** Status code. */
        public final int code;

        /** Parsed body. */
        public final Map<String, Object> body;

        /**
         * @param code Status code.
         * @param body Parsed body.
         */
        Response(int code, Map<String, Object> body) {
            this.code = code;
            this.body = body;
        }
    }
}
