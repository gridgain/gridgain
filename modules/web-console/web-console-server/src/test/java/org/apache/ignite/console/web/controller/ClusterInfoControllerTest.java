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

import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.console.dto.ClusterInfo;
import org.apache.ignite.console.repositories.ClusterInfoRepository;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Cluster info controller test.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@WithMockUser(username = "u", password = "p", roles = "USER")
public class ClusterInfoControllerTest {
    /** Mock mvc. */
    @Autowired
    private MockMvc mockMvc;

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
     * Should return json array with cluster info.
     */
    @Test
    public void loadClustersInfo() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo(UUID.fromString("a-a-a-a-a"), 123);
        repo.save(clusterInfo);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/v1/cluster/list"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().string("[{\"id\":\"0000000a-000a-000a-000a-00000000000a\",\"lastSeen\":123}]"));
    }
}
