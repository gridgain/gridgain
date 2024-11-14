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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.internal.IgniteVersionUtils;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

/**
 * This class is responsible for getting GridGain updates information via HTTP
 */
public class HttpIgniteUpdatesChecker {
    /** Url for request updates. */
    private final String url;

    /** Charset for encoding requests/responses */
    private final String charset;

    /**
     * Creates new HTTP Ignite updates checker with following parameters
     * @param url URL for getting Ignite updates information
     * @param charset Charset for encoding
     */
    HttpIgniteUpdatesChecker(String url, String charset) {
        this.url = url;
        this.charset = charset;
    }

    /**
     * Gets information about Ignite updates via HTTP
     * @param updateReq HTTP Request parameters
     * @return Information about Ignite updates separated by line endings
     * @throws IOException If HTTP request was failed
     */
    public String getUpdates(String updateReq) throws IOException {
        return getUpdates2();
    }

    private String getUpdates2() throws IOException {
        URL url1 = new URL(url);
        int port = url1.getPort();

        if (port == -1) {
            port = url1.getDefaultPort();
        }

        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        try (
                SSLSocket socket = (SSLSocket) factory.createSocket(url1.getHost(), port);
                OutputStream os = socket.getOutputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send HTTP GET request
            String request = "GET " + url1.getPath() + " HTTP/1.1\r\n" +
                    "Host: " + url1.getHost() + "\r\n" +
                    "Connection: close\r\n" +
                    "{\"product\": \"gg\", \"version\": \"8.9.12\"}" +
                    "\r\n";

            // TODO
            // String requestBody = "{\"product\": \"gg\", \"version\": \"" + IgniteVersionUtils.VER_STR +  "\"}";

            os.write(request.getBytes());
            os.flush();

            // Read the response
            StringBuilder res = new StringBuilder();

            for (String line; (line = reader.readLine()) != null; )
                res.append(line).append('\n');

            return res.toString();
        }
    }
}
