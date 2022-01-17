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

import javax.management.JMException;
import java.util.UUID;

/**
 * MX Bean allows to access information about cluster ID and tag and change tag.
 */
@MXBeanDescription("MBean that provides access to information about cluster ID and tag.")
public interface IgniteClusterMXBean {
    /**
     * Gets cluster ID.
     *
     * @return Cluster ID.
     */
    @MXBeanDescription("Unique identifier of the cluster.")
    public UUID getId();

    /**
     * Changes cluster ID to provided value.
     *
     * @param newId New value to be set as cluster ID.
     */
    @MXBeanDescription("Set new cluster ID value.")
    public void id(@MXBeanParameter(name = "newId", description = "New ID value to be set.") UUID newId);

    /**
     * Gets current cluster tag.
     *
     * @return Current cluster tag.
     */
    @MXBeanDescription("User-defined cluster tag.")
    public String getTag();

    /**
     * Changes cluster tag to provided value.
     *
     * @param newTag New value to be set as cluster tag.
     * @throws JMException This exception is never thrown. The declaration is kept for source compatibility.
     */
    @MXBeanDescription("Set new cluster tag value.")
    public void tag(@MXBeanParameter(name = "newTag", description = "New tag value to be set.") String newTag)
        throws JMException;
}
