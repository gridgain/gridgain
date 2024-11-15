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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public Map<String, String> getUpdates(Map<String, Object> updateReq) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

        conn.setDoOutput(true);

        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "*/*");
        conn.setRequestProperty("user-agent", "Foo");

        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try (OutputStream os = conn.getOutputStream()) {
            StringBuilder bodyBuilder = new StringBuilder("{");
            bodyBuilder.append("\"product\": \"gg\", \"version\": \"")
                    .append(IgniteVersionUtils.VER_STR)
                    .append("\", \"instanceData\": {");

            for (Map.Entry<String, Object> entry : updateReq.entrySet()) {
                bodyBuilder
                        .append("\"").append(entry.getKey()).append("\": ");

                if (entry.getValue() == null) {
                    bodyBuilder.append("null, ");
                }
                else {
                    bodyBuilder
                            .append("\"")
                            .append(escapeJson(entry.getValue().toString()))
                            .append("\", ");
                }
            }

            bodyBuilder.delete(bodyBuilder.length() - 2, bodyBuilder.length());
            bodyBuilder.append("}}");

            os.write(bodyBuilder.toString().getBytes(charset));
            os.flush();
        }

        try (InputStream in = conn.getInputStream()) {
            if (in == null)
                return null;

            BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset));

            Map<String, String> res = new HashMap<>();

            for (String line; (line = reader.readLine()) != null; ) {
                String[] parts = line.split("=", 2);

                if (parts.length == 2) {
                    res.put(parts[0].trim(), parts[1].trim());
                } else {
                    res.put(line.trim(), null);
                }
            }

            return res;
        }
    }

    private static String escapeJson(String str) {
        // https://www.ietf.org/rfc/rfc4627.txt
        // All Unicode characters may be placed within the quotation marks except for the characters that must be escaped:
        // quotation mark, reverse solidus, and the control characters (U+0000 through U+001F).
        return str
                .replaceAll("[\u0000-\u001F]", "")
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
