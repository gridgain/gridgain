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

package org.apache.ignite.internal.processors.security.impl;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * Security context for tests.
 */
public class TestSecurityContext implements SecurityContext, Serializable {
    /** Subject. */
    private final SecuritySubject subject;

    /**
     * @param subject Subject.
     */
    public TestSecurityContext(SecuritySubject subject) {
        this.subject = subject;
    }

    /**
     * @param opName Op name.
     * @param perm Permission.
     */
    public boolean operationAllowed(String opName, SecurityPermission perm) {
        switch (perm) {
            case CACHE_CREATE:
            case CACHE_DESTROY:
                return systemOperationAllowed(perm) || cacheOperationAllowed(opName, perm);

            case CACHE_PUT:
            case CACHE_READ:
            case CACHE_REMOVE:
                return cacheOperationAllowed(opName, perm);

            case TASK_CANCEL:
            case TASK_EXECUTE:
                return taskOperationAllowed(opName, perm);

            case SERVICE_DEPLOY:
            case SERVICE_INVOKE:
            case SERVICE_CANCEL:
                return serviceOperationAllowed(opName, perm);

            case TRACING_CONFIGURATION_UPDATE:
                return tracingOperationAllowed(perm);

            case EVENTS_DISABLE:
            case EVENTS_ENABLE:
            case ADMIN_VIEW:
            case ADMIN_CACHE:
            case ADMIN_QUERY:
            case ADMIN_OPS:
            case JOIN_AS_SERVER:
                return systemOperationAllowed(perm);

            default:
                throw new IllegalStateException("Invalid security permission: " + perm);
        }
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject subject() {
        return subject;
    }

    /** {@inheritDoc} */
    @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
        return hasPermission(subject.permissions().taskPermissions().get(taskClsName), perm);
    }

    /** {@inheritDoc} */
    @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
        return hasPermission(subject.permissions().cachePermissions().get(cacheName), perm);
    }

    /** {@inheritDoc} */
    @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
        return hasPermission(subject.permissions().servicePermissions().get(srvcName), perm);
    }

    /** {@inheritDoc} */
    @Override public boolean tracingOperationAllowed(SecurityPermission perm) {
        return hasPermission(subject.permissions().tracingPermissions(), perm);
    }

    /** {@inheritDoc} */
    @Override public boolean systemOperationAllowed(SecurityPermission perm) {
        Collection<SecurityPermission> perms = subject.permissions().systemPermissions();

        if (F.isEmpty(perms))
            return subject.permissions().defaultAllowAll();

        return perms.stream().anyMatch(p -> perm == p);
    }

    /**
     * @param perms Permissions.
     * @param perm Permission.
     */
    private boolean hasPermission(Collection<SecurityPermission> perms, SecurityPermission perm) {
        if (perms == null)
            return subject.permissions().defaultAllowAll();

        return perms.stream().anyMatch(p -> perm == p);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TestSecurityContext{" +
            "subject=" + subject +
            '}';
    }
}
