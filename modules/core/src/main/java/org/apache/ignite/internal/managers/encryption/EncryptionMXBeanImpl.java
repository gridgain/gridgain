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

package org.apache.ignite.internal.managers.encryption;

import java.util.Collections;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.mxbean.EncryptionMXBean;

/**
 * Encryption features MBean.
 */
public class EncryptionMXBeanImpl implements EncryptionMXBean {
    /** Encryption manager. */
    private final GridEncryptionManager encryptionMgr;

    /** @param ctx Context. */
    public EncryptionMXBeanImpl(GridKernalContextImpl ctx) {
        encryptionMgr = ctx.encryption();
    }

    /** {@inheritDoc} */
    @Override public String getMasterKeyName() {
        return encryptionMgr.getMasterKeyName();
    }

    /** {@inheritDoc} */
    @Override public void changeMasterKey(String masterKeyName) {
        encryptionMgr.changeMasterKey(masterKeyName).get();
    }

    /** {@inheritDoc} */
    @Override public void changeCacheGroupKey(String cacheOrGrpName) {
        encryptionMgr.changeCacheGroupKey(Collections.singleton(cacheOrGrpName)).get();
    }
}
