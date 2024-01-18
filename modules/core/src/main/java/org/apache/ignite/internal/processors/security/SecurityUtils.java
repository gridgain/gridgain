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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Security utilities.
 */
public class SecurityUtils {
    /** Default serialization version. */
    private static final int DFLT_SERIALIZE_VERSION = isSecurityCompatibilityMode() ? 1 : 3;

    /** Current serialization version. */
    private static final ThreadLocal<Integer> SERIALIZE_VERSION = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return DFLT_SERIALIZE_VERSION;
        }
    };

    /**
     * Private constructor.
     */
    private SecurityUtils() {
    }

    /**
     * @return Security compatibility mode flag.
     */
    public static boolean isSecurityCompatibilityMode() {
        return IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SECURITY_COMPATIBILITY_MODE, false);
    }

    /**
     * @param ver Serialize version.
     */
    public static void serializeVersion(int ver) {
        SERIALIZE_VERSION.set(ver);
    }

    /**
     * @return Serialize version.
     */
    public static int serializeVersion() {
        return SERIALIZE_VERSION.get();
    }

    /**
     * Sets default serialize version {@link #DFLT_SERIALIZE_VERSION}.
     */
    public static void restoreDefaultSerializeVersion() {
        serializeVersion(DFLT_SERIALIZE_VERSION);
    }

    /**
     * @return Allow all service permissions.
     */
    public static Map<String, Collection<SecurityPermission>> compatibleServicePermissions() {
        Map<String, Collection<SecurityPermission>> srvcPerms = new HashMap<>();

        srvcPerms.put("*", Arrays.asList(
            SecurityPermission.SERVICE_CANCEL,
            SecurityPermission.SERVICE_DEPLOY,
            SecurityPermission.SERVICE_INVOKE));

        return srvcPerms;
    }

    /**
     * Gets the node's security context.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @param node Node.
     * @return Node's security context.
     */
    public static SecurityContext nodeSecurityContext(Marshaller marsh, ClassLoader ldr, ClusterNode node) {
        A.notNull(node, "node");

        byte[] subjBytesV3 = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V3);
        byte[] subjBytesV2 = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT_V2);
        byte[] subjBytes = node.attribute(IgniteNodeAttributes.ATTR_SECURITY_SUBJECT);

        if (subjBytes == null && subjBytesV2 == null && subjBytesV3 == null)
            throw new SecurityException("Security context isn't certain.");


        try {
            if (subjBytesV3 != null)
                return U.unmarshal(marsh, subjBytesV3, ldr);

            if (subjBytesV2 != null)
                return nodeSecurityContext0(marsh, subjBytesV2, ldr, 2);

            return nodeSecurityContext0(marsh, subjBytes, ldr, 1);
        }
        catch (IgniteCheckedException e) {
            throw new SecurityException("Failed to get security context. [node=" + node.consistentId() + ", id=" + node.id() +
                ", bytesV3=" + (subjBytesV3!=null) + ", bytesV2=" + (subjBytesV2!=null) + ", bytesV1=" + (subjBytes!=null) + ']', e);
        }
    }

    private static SecurityContext nodeSecurityContext0(
        Marshaller marsh,
        byte[] subjBytes,
        ClassLoader ldr,
        int ver
    ) throws IgniteCheckedException {
        try {
            serializeVersion(ver);

            return U.unmarshal(marsh, subjBytes, ldr);
        }
        finally {
            restoreDefaultSerializeVersion();
        }
    }
}
