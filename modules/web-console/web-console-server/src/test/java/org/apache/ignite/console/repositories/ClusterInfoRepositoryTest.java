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

package org.apache.ignite.console.repositories;

import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.console.dto.ClusterInfo;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test for clusters info repository.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ClusterInfoRepositoryTest {
    /** Cluster info repository. */
    @Autowired
    private ClusterInfoRepository repo;

    /**
     * Remove all entries from table.
     */
    @After
    public void cleanup() {
        Set<UUID> ids = repo.list().stream().map(AbstractDto::getId).collect(Collectors.toSet());
        repo.deleteAll(ids);
    }

    /**
     * Should save cluster info.
     */
    @Test
    public void testSave() {
        ClusterInfo clusterInfo = new ClusterInfo(UUID.randomUUID(), System.currentTimeMillis());
        repo.save(clusterInfo);

        List<ClusterInfo> clustersInfo = repo.list();

        assertEquals(clusterInfo.getId(), clustersInfo.get(0).getId());
    }

    /**
     * Should load clusters info.
     */
    @Test
    public void testLoad() {
        ClusterInfo clusterInfo_1 = new ClusterInfo(UUID.randomUUID(), System.currentTimeMillis());
        ClusterInfo clusterInfo_2 = new ClusterInfo(UUID.randomUUID(), System.currentTimeMillis());
        repo.save(clusterInfo_1);
        repo.save(clusterInfo_2);

        List<ClusterInfo> clustersInfo = repo.list();

        assertEquals(2, clustersInfo.size());
    }
}
