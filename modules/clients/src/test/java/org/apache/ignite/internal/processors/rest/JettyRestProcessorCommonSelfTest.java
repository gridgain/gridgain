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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;

/**
 * Base class for testing Jetty REST protocol.
 */
public abstract class JettyRestProcessorCommonSelfTest extends AbstractRestProcessorSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** JSON to java mapper. */
    protected static final ObjectMapper JSON_MAPPER = new GridJettyObjectMapper();

    /** Rest client. */
    private final TestRestClient restClient = createRestClient();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_JETTY_PORT, Integer.toString(restPort()));

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @return Rest client, you may override some function of it.
     */
    protected TestRestClient createRestClient() {
        return new TestRestClient(this::signature);
    }

    /**
     * @return Port to use for rest. Needs to be changed over time because Jetty has some delay before port unbind.
     */
    protected int restPort() {
        return restClient.restPort();
    }

    /**
     * @return Security enabled flag. Should be the same with {@code ctx.security().enabled()}.
     */
    protected boolean securityEnabled() {
        return false;
    }

    /**
     * @return Signature.
     * @throws Exception If failed.
     */
    protected abstract String signature() throws Exception;

    /**
     * Execute REST command and return result.
     *
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    protected String content(Map<String, String> params) throws Exception {
        return restClient.content(params);
    }

    /**
     * @param cacheName Optional cache name.
     * @param cmd REST command.
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    protected String content(String cacheName, GridRestCommand cmd, String... params) throws Exception {
        return restClient.content(cacheName, cmd, params);
    }

    /**
     * @param json JSON content.
     * @param field Field name in JSON object.
     * @return Field value.
     * @throws IOException If failed.
     */
    protected String jsonField(String json, String field) throws IOException {
       return restClient.jsonField(json, field);
    }
}
