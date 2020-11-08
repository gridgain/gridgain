/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.springdata22.repository.config;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.springdata22.repository.IgniteRepository;
import org.apache.ignite.springdata22.repository.support.IgniteRepositoryFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;

/**
 * Apache Ignite specific implementation of {@link RepositoryConfigurationExtension}.
 */
public class IgniteRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {
    /** {@inheritDoc} */
    @Override public String getModuleName() {
        return "Apache Ignite";
    }

    /** {@inheritDoc} */
    @Override protected String getModulePrefix() {
        return "ignite";
    }

    /** {@inheritDoc} */
    @Override public String getRepositoryFactoryBeanClassName() {
        return IgniteRepositoryFactoryBean.class.getName();
    }

    /** {@inheritDoc} */
    @Override protected Collection<Class<?>> getIdentifyingTypes() {
        return Collections.<Class<?>>singleton(IgniteRepository.class);
    }
}
