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

package org.apache.ignite.springdata.repository.support;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * Apache Ignite repository factory bean.
 *
 * The repository requires to define one of the parameters below in your Spring application configuration in order
 * to get an access to Apache Ignite cluster:
 * <ul>
 * <li>{@link Ignite} or {@link IgniteClient} instance bean named "igniteInstance"</li>
 * <li>{@link IgniteConfiguration} or {@link ClientConfiguration} bean named "igniteCfg"</li>
 * <li>A path to Ignite's Spring XML configuration named "igniteSpringCfgPath"</li>
 * <ul/>
 *
 * @param <T> Repository type, {@link IgniteRepository}
 * @param <S> Domain object class.
 * @param <ID> Domain object key, super expects {@link Serializable}.
 */
public class IgniteRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable>
    extends RepositoryFactoryBeanSupport<T, S, ID> implements ApplicationContextAware {
    /** Application context. */
    private ApplicationContext ctx;

    /**
     * @param repositoryInterface Repository interface.
     */
    protected IgniteRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext context) throws BeansException {
        this.ctx = context;
    }

    /** {@inheritDoc} */
    @Override protected RepositoryFactorySupport createRepositoryFactory() {
        try {
            Object igniteInstanceBean = ctx.getBean("igniteInstance");

            if (igniteInstanceBean instanceof Ignite)
                return new IgniteRepositoryFactory((Ignite)igniteInstanceBean);
            else if (igniteInstanceBean instanceof IgniteClient)
                return new IgniteRepositoryFactory((IgniteClient)igniteInstanceBean);

            throw new IllegalStateException("Invalid repository configuration. The Spring Bean corresponding to the" +
                " \"igniteInstance\" property of repository configuration must be one of the following types: " +
                Ignite.class.getName() + ", " + IgniteClient.class.getName());
        }
        catch (BeansException ex) {
            try {
                Object igniteCfgBean = ctx.getBean("igniteCfg");

                if (igniteCfgBean instanceof IgniteConfiguration)
                    return new IgniteRepositoryFactory((IgniteConfiguration)igniteCfgBean);
                else if (igniteCfgBean instanceof ClientConfiguration)
                    return new IgniteRepositoryFactory((ClientConfiguration)igniteCfgBean);

                throw new IllegalStateException("Invalid repository configuration. The Spring Bean corresponding to" +
                    " the \"igniteCfg\" property of repository configuration must be one of the following types: [" +
                    IgniteConfiguration.class.getName() + ", " + ClientConfiguration.class.getName() + ']');
            }
            catch (BeansException ex2) {
                try {
                    String path = (String)ctx.getBean("igniteSpringCfgPath");

                    return new IgniteRepositoryFactory(path);
                }
                catch (BeansException ex3) {
                    throw new IgniteException("Failed to initialize Ignite repository factory. One of the following" +
                        " beans must be defined in application configuration: \"igniteInstance\", \"igniteCfg\"," +
                        " \"igniteSpringCfgPath\".");
                }
            }
        }
    }
}

