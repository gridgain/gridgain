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

package org.apache.ignite.console.agent.rest;

import java.io.IOException;
import java.io.StringWriter;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.LoggerFactory;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;
import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * API to execute REST requests to Ignite cluster.
 */
public class RestExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestExecutor.class));

    /** */
    private final AgentConfiguration cfg;

    /** */
    private final HttpClient httpClient;

    /** Index of alive node URI. */
    private final Map<List<String>, Integer> startIdxs = U.newHashMap(2);

    /**
     * Constructor.
     *
     * @param cfg Config.
     * @throws Exception If failed to start HTTP client.
     */
    public RestExecutor(AgentConfiguration cfg) throws Exception {
        this.cfg = cfg;
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.nodeTrustStore())) {
            log.warning("Options contains both '--node-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to cluster.");

            trustAll = false;
        }

        boolean ssl = trustAll || !F.isEmpty(cfg.nodeTrustStore()) || !F.isEmpty(cfg.nodeKeyStore());

        if (ssl) {
            SslContextFactory sslCtxFactory = sslContextFactory(
                cfg.nodeKeyStore(),
                cfg.nodeKeyStorePassword(),
                trustAll,
                cfg.nodeTrustStore(),
                cfg.nodeTrustStorePassword(),
                cfg.cipherSuites()

            );

            httpClient = new HttpClient(sslCtxFactory);
        }
        else
            httpClient = new HttpClient();

        httpClient.start();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            httpClient.stop();
        }
        catch (Throwable e) {
            log.error("Failed to close HTTP client", e);
        }
    }

    /**
     * @param res Response from cluster.
     * @return Result of REST request.
     * @throws Exception If failed to parse REST result.
     */
    private RestResult parseResponse(ContentResponse res) throws Exception {
        int code = res.getStatus();

        if (code == HTTP_OK) {
            RestResponseHolder holder = fromJson(res.getContent(), RestResponseHolder.class);

            int status = holder.getSuccessStatus();

            return status == STATUS_SUCCESS
                ? RestResult.success(holder.getResponse(), holder.getSessionToken())
                : RestResult.fail(status, holder.getError());
        }

        if (code == HTTP_UNAUTHORIZED) {
            return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in cluster. " +
                "Please check agent\'s login and password or node port.");
        }

        if (code == HTTP_NOT_FOUND)
            return RestResult.fail(STATUS_FAILED, "Failed connect to cluster.");

        return RestResult.fail(STATUS_FAILED, "Failed to execute REST command [code=" +
            code + ", msg=" + res.getReason() + "]");
    }

    /** */
    private RestResult sendRequest(String url, JsonObject params) throws Throwable {
        Request req = httpClient
            .newRequest(url)
            .path("/ignite")
            .method(HttpMethod.POST);

        params.forEach((k, v) -> req.param(k, String.valueOf(v)));

        ContentResponse res = req.send();

        return parseResponse(res);
    }

    /**
     * Send request to cluster.
     *
     * @param params Map with request params.
     * @return Response from cluster.
     * @throws ConnectException if failed to connect to cluster.
     */
    public RestResult sendRequest(JsonObject params) throws ConnectException {
        List<String> nodeURIs = cfg.nodeURIs();

        Integer startIdx = startIdxs.getOrDefault(nodeURIs, 0);

        int urlsCnt = nodeURIs.size();

        for (int i = 0;  i < urlsCnt; i++) {
            int currIdx = (startIdx + i) % urlsCnt;

            String nodeUrl = nodeURIs.get(currIdx);

            try {
                RestResult res = sendRequest(nodeUrl, params);

                // If first attempt failed then throttling should be cleared.
                if (i > 0)
                    LT.clear();

                LT.info(log, "Connected to cluster [url=" + nodeUrl + "]");

                startIdxs.put(nodeURIs, currIdx);

                return res;
            }
            catch (Throwable e) {
                if (log.isDebugEnabled())
                    log.error("Failed connect to cluster [url=" + nodeUrl + "]", e);
                else
                    LT.warn(log, "Failed connect to cluster [url=" + nodeUrl + "]");
            }
        }

        LT.warn(log, "Failed connect to cluster. " +
            "Please ensure that nodes have [ignite-rest-http] module in classpath " +
            "(was copied from libs/optional to libs folder).");

        throw new ConnectException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]");
    }

    /**
     * REST response holder Java bean.
     */
    private static class RestResponseHolder {
        /** Success flag */
        private int successStatus;

        /** Error. */
        private String err;

        /** Response. */
        private String res;

        /** Session token string representation. */
        private String sesTok;

        /**
         * @return {@code True} if this request was successful.
         */
        public int getSuccessStatus() {
            return successStatus;
        }

        /**
         * @param successStatus Whether request was successful.
         */
        public void setSuccessStatus(int successStatus) {
            this.successStatus = successStatus;
        }

        /**
         * @return Error.
         */
        public String getError() {
            return err;
        }

        /**
         * @param err Error.
         */
        public void setError(String err) {
            this.err = err;
        }

        /**
         * @return Response object.
         */
        public String getResponse() {
            return res;
        }

        /**
         * @param res Response object.
         */
        @JsonDeserialize(using = RawContentDeserializer.class)
        public void setResponse(String res) {
            this.res = res;
        }

        /**
         * @return String representation of session token.
         */
        public String getSessionToken() {
            return sesTok;
        }

        /**
         * @param sesTok String representation of session token.
         */
        public void setSessionToken(String sesTok) {
            this.sesTok = sesTok;
        }
    }

    /**
     * Raw content deserializer that will deserialize any data as string.
     */
    private static class RawContentDeserializer extends JsonDeserializer<String> {
        /** */
        private final JsonFactory factory = new JsonFactory();

        /**
         * @param tok Token to process.
         * @param p Parser.
         * @param gen Generator.
         */
        private void writeToken(JsonToken tok, JsonParser p, JsonGenerator gen) throws IOException {
            switch (tok) {
                case FIELD_NAME:
                    gen.writeFieldName(p.getText());
                    break;

                case START_ARRAY:
                    gen.writeStartArray();
                    break;

                case END_ARRAY:
                    gen.writeEndArray();
                    break;

                case START_OBJECT:
                    gen.writeStartObject();
                    break;

                case END_OBJECT:
                    gen.writeEndObject();
                    break;

                case VALUE_NUMBER_INT:
                    gen.writeNumber(p.getBigIntegerValue());
                    break;

                case VALUE_NUMBER_FLOAT:
                    gen.writeNumber(p.getDecimalValue());
                    break;

                case VALUE_TRUE:
                    gen.writeBoolean(true);
                    break;

                case VALUE_FALSE:
                    gen.writeBoolean(false);
                    break;

                case VALUE_NULL:
                    gen.writeNull();
                    break;

                default:
                    gen.writeString(p.getText());
            }
        }

        /** {@inheritDoc} */
        @Override public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken startTok = p.getCurrentToken();

            if (startTok.isStructStart()) {
                StringWriter wrt = new StringWriter(4096);

                JsonGenerator gen = factory.createGenerator(wrt);

                JsonToken tok = startTok, endTok = startTok == START_ARRAY ? END_ARRAY : END_OBJECT;

                int cnt = 1;

                while (cnt > 0) {
                    writeToken(tok, p, gen);

                    tok = p.nextToken();

                    if (tok == startTok)
                        cnt++;
                    else if (tok == endTok)
                        cnt--;
                }

                gen.close();

                return wrt.toString();
            }

            return p.getValueAsString();
        }
    }
}
