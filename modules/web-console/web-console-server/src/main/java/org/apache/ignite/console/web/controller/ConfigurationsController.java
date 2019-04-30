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

import java.util.UUID;
import io.swagger.annotations.ApiOperation;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.apache.ignite.console.common.Utils.idsFromJson;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for configurations API.
 */
@RestController
@RequestMapping(path = "/api/v1/configuration")
public class ConfigurationsController {
    /** */
    private final ConfigurationsService cfgsSrvc;

    /**
     * @param cfgsSrvc Configurations service.
     */
    public ConfigurationsController(ConfigurationsService cfgsSrvc) {
        this.cfgsSrvc = cfgsSrvc;
    }

    /**
     * @param acc Account.
     * @param clusterId Cluster ID.
     */
    @ApiOperation(value = "Get full cluster object.")
    @GetMapping(path = "/{clusterId}")
    public ResponseEntity<JsonObject> loadConfiguration(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadConfiguration(acc.getId(), clusterId));
    }

    /**
     * @param acc Account.
     * @return Clusters short list.
     */
    @ApiOperation(value = "Clusters short list.")
    @GetMapping(path = "/clusters")
    public ResponseEntity<JsonArray> loadClustersShortList(@AuthenticationPrincipal Account acc) {
        return ResponseEntity.ok(cfgsSrvc.loadClusters(acc.getId()));
    }

    /**
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Cluster as JSON.
     */
    @ApiOperation(value = "Get cluster configuration.")
    @GetMapping(path = "/clusters/{clusterId}")
    public ResponseEntity<String> loadCluster(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadCluster(acc.getId(), clusterId));
    }

    /**
     * Load cluster caches short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Caches short list.
     */
    @ApiOperation(value = "Caches short list.")
    @GetMapping(path = "/clusters/{clusterId}/caches")
    public ResponseEntity<JsonArray> loadCachesShortList(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadShortCaches(acc.getId(), clusterId));
    }

    /**
     * Load cluster models short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Models short list.
     */
    @ApiOperation(value = "Get models short list.")
    @GetMapping(path = "/clusters/{clusterId}/models")
    public ResponseEntity<JsonArray> loadModelsShortList(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadShortModels(acc.getId(), clusterId));
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return IGFSs short list.
     */
    @ApiOperation(value = "Get IGFSs short list.")
    @GetMapping(path = "/clusters/{clusterId}/igfss")
    public ResponseEntity<JsonArray> loadIgfssShortList(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadShortIgfss(acc.getId(), clusterId));
    }

    /**
     * @param acc Account.
     * @param cacheId Cache ID.
     */
    @ApiOperation(value = "Get cache configuration.")
    @GetMapping(path = "/caches/{cacheId}")
    public ResponseEntity<String> loadCache(
        @AuthenticationPrincipal Account acc,
        @PathVariable("cacheId") UUID cacheId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadCache(acc.getId(), cacheId));
    }

    /**
     * @param acc Account.
     * @param mdlId Model ID.
     */
    @ApiOperation(value = "Get model configuration.")
    @GetMapping(path = "/domains/{modelId}")
    public ResponseEntity<String> loadModel(
        @AuthenticationPrincipal Account acc,
        @PathVariable("modelId") UUID mdlId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadModel(acc.getId(), mdlId));
    }

    /**
     * @param igfsId IGFS ID.
     */
    @ApiOperation(value = "Get IGFS configuration.")
    @GetMapping(path = "/igfs/{igfsId}")
    public ResponseEntity<String> loadIgfs(
        @AuthenticationPrincipal Account acc,
        @PathVariable("igfsId") UUID igfsId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadIgfs(acc.getId(), igfsId));
    }

    /**
     * Save cluster.
     *
     * @param acc Account.
     * @param changedItems Items to save.
     */
    @ApiOperation(value = "Save cluster advanced configuration.")
    @PutMapping(path = "/clusters", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> saveAdvancedCluster(
        @AuthenticationPrincipal Account acc,
        @RequestBody JsonObject changedItems
    ) {
        cfgsSrvc.saveAdvancedCluster(acc.getId(), changedItems);

        return ResponseEntity.ok().build();
    }

    /**
     * Save basic clusters.
     *
     * @param acc Account.
     * @param changedItems Items to save.
     */
    @ApiOperation(value = "Save cluster basic configuration.")
    @PutMapping(path = "/clusters/basic", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> saveBasicCluster(
        @AuthenticationPrincipal Account acc,
        @RequestBody JsonObject changedItems
    ) {
        cfgsSrvc.saveBasicCluster(acc.getId(), changedItems);

        return ResponseEntity.ok().build();
    }

    /**
     * Delete clusters.
     *
     * @param acc Account.
     * @param clusterIDs Cluster IDs for removal.
     */
    @ApiOperation(value = "Delete cluster   .")
    @PostMapping(path = "/clusters/remove", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> deleteClusters(
        @AuthenticationPrincipal Account acc,
        @RequestBody JsonObject clusterIDs
    ) {
        cfgsSrvc.deleteClusters(acc.getId(), idsFromJson(clusterIDs, "clusterIDs"));

        return ResponseEntity.ok().build();
    }
}
