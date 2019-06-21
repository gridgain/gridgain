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

package org.apache.ignite.console.web.controller;

import javax.cache.CacheException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.console.web.model.ErrorResponse;
import org.apache.ignite.console.web.model.ErrorWithEmailResponse;
import org.apache.ignite.console.web.security.MissingConfirmRegistrationException;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import static org.apache.ignite.console.common.Utils.errorMessage;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

/**
 * REST exceptions handler.
 */
@ControllerAdvice
public class RestExceptionHandler extends ResponseEntityExceptionHandler {
    /** */
    private static final int EMAIL_NOT_CONFIRMED = 10104;

    /**
     * Handles account disabled exceptions.
     *
     * @param ex Service exception.
     * @param req Web request.
     * @return {@link ErrorResponse} instance with error code and message.
     */
    @ExceptionHandler(value = {MissingConfirmRegistrationException.class})
    protected ResponseEntity<Object> handleDisabledAccountException(MissingConfirmRegistrationException ex, WebRequest req) {
        return handleExceptionInternal(ex,
            new ErrorWithEmailResponse(EMAIL_NOT_CONFIRMED, errorMessage(ex), ex.getUsername()), null, FORBIDDEN, req);
    }

    /**
     * Handles authentication exceptions.
     *
     * @param ex Service exception.
     * @param req Web request.
     * @return {@link ErrorResponse} instance with error code and message.
     */
    @ExceptionHandler(value = {AuthenticationException.class})
    protected ResponseEntity<Object> handleAuthException(AuthenticationException ex, WebRequest req) {
        return handleExceptionInternal(ex, new ErrorResponse(FORBIDDEN, errorMessage(ex)), null, FORBIDDEN, req);
    }

    /**
     * Handles cache exceptions.
     *
     * @param ex Service exception.
     * @param req Web request.
     * @return {@link ErrorResponse} instance with error code and message.
     */
    @ExceptionHandler(value = {CacheException.class})
    protected ResponseEntity<Object> handleClientDisconnected(CacheException ex, WebRequest req) {
        return handleExceptionInternal(ex, new ErrorResponse(SERVICE_UNAVAILABLE, "Database error"), null, SERVICE_UNAVAILABLE, req);
    }

    /**
     * Handles client disconnected exceptions.
     *
     * @param ex Service exception.
     * @param req Web request.
     * @return {@link ErrorResponse} instance with error code and message.
     */
    @ExceptionHandler(value = {IgniteClientDisconnectedException.class})
    protected ResponseEntity<Object> handleClientDisconnected(IgniteClientDisconnectedException ex, WebRequest req) {
        return handleExceptionInternal(ex, new ErrorResponse(SERVICE_UNAVAILABLE, "Database not available"), null, SERVICE_UNAVAILABLE, req);
    }

    /**
     * Handles all other exceptions.
     *
     * @param ex Service exception.
     * @param req Web request.
     * @return {@link ErrorResponse} instance with error code and message.
     */
    @ExceptionHandler(value = {Exception.class})
    protected ResponseEntity<Object> handleGenericException(Exception ex, WebRequest req) {
        return handleExceptionInternal(ex, new ErrorResponse(INTERNAL_SERVER_ERROR, errorMessage(ex)),
            null, INTERNAL_SERVER_ERROR, req);
    }
}
