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

import _ from 'lodash';

/**
 * Check persistence enabled in specified configuration.
 *
 * @param {Object} cfg Configuration to check.
 */
export function isPersistenceEnabled(cfg) {
    const storeCfg = _.get(cfg, 'dataStorageConfiguration');

    return !!storeCfg
        && (_.get(storeCfg.defaultDataRegionConfiguration, 'persistenceEnabled')
            || !!_.find(storeCfg.dataRegionConfigurations, (dsCfg) => dsCfg.persistenceEnabled));
}
