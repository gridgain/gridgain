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

package org.apache.ignite.console.services;

import org.apache.ignite.console.dto.ClusterInfo;
import org.apache.ignite.console.repositories.ClusterInfoRepository;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Cluster info service.
 */
@Service
public class ClusterInfoService {
    /** Cluster info repository. */
    private final ClusterInfoRepository clusterInfoRepo;

    /**
     * @param clusterInfoRepo Cluster info repository.
     */
    public ClusterInfoService(ClusterInfoRepository clusterInfoRepo) {
        this.clusterInfoRepo = clusterInfoRepo;
    }

    /**
     * @return Clusters info.
     */
    public List<ClusterInfo> list() {
        return clusterInfoRepo.list();
    }
}
