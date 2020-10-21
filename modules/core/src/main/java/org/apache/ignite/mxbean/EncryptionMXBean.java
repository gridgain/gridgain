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
package org.apache.ignite.mxbean;

import org.apache.ignite.IgniteEncryption;

/**
 * Encryption features MBean.
 */
@MXBeanDescription("MBean that provides access to encryption features.")
public interface EncryptionMXBean {
    /**
     * Gets the current master key name.
     *
     * @return Master key name.
     * @see IgniteEncryption#getMasterKeyName()
     */
    @MXBeanDescription("Current master key name.")
    public String getMasterKeyName();

    /**
     * Starts master key change process.
     *
     * @param masterKeyName Master key name.
     * @see IgniteEncryption#changeMasterKey(String)
     */
    @MXBeanDescription("Change master key name.")
    @MXBeanParametersNames("masterKeyName")
    @MXBeanParametersDescriptions("Master key name.")
    public void changeMasterKey(String masterKeyName);
}
