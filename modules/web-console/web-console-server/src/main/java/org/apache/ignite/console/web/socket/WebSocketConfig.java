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

package org.apache.ignite.console.web.socket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistration;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import static org.apache.ignite.console.websocket.WebSocketEvents.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSERS_PATH;

/**
 * Websocket configuration.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
	/** */
	private final AgentsHandler agentsHandler;

	/** */
	private final BrowsersHandler browsersHandler;

	/** */
	@Value("${websocket.cors.enabled:false}")
	private boolean corsEnabled;

	/** */
	@Value("${websocket.allowed.origin:https://localhost}")
	private String allowedOrigin;

	/**
	 * @param agentsHandler Agents handler.
	 * @param browsersHandler Browsers handler.
	 */
	public WebSocketConfig(AgentsHandler agentsHandler, BrowsersHandler browsersHandler) {
		this.agentsHandler = agentsHandler;
		this.browsersHandler = browsersHandler;
	}

	/**
	 * @param registry Registry.
	 */
	@Override public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry.addHandler(agentsHandler, AGENTS_PATH);

		WebSocketHandlerRegistration browsersReg = registry.addHandler(browsersHandler, BROWSERS_PATH);

		if (corsEnabled)
			browsersReg.setAllowedOrigins(allowedOrigin);
	}
}
