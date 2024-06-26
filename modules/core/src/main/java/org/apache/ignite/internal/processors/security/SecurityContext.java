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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * This interface should be used to get security subject and perform checks for specific permissions.
 */
public interface SecurityContext {
    /**
     * @return Security subject.
     */
    public SecuritySubject subject();

    /**
     * Checks whether task operation is allowed.
     *
     * @param taskClsName Task class name.
     * @param perm Permission to check.
     * @return {@code True} if task operation is allowed.
     */
    public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm);

    /**
     * Checks whether cache operation is allowed.
     *
     * @param cacheName Cache name.
     * @param perm Permission to check.
     * @return {@code True} if cache operation is allowed.
     */
    public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm);

    /**
     * Checks whether service operation is allowed.
     *
     * @param srvcName Service name.
     * @param perm Permission to check.
     * @return {@code True} if task operation is allowed.
     */
    public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm);

    /**
     * Checks whether tracing operation is allowed.
     *
     * @param perm Permission to check.
     * @return {@code True} if tracing operation is allowed.
     */
    public boolean tracingOperationAllowed(SecurityPermission perm);

    /**
     * Checks whether system-wide permission is allowed (excluding Visor task operations).
     *
     * @param perm Permission to check.
     * @return {@code True} if system operation is allowed.
     */
    public boolean systemOperationAllowed(SecurityPermission perm);
}