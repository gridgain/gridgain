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

package org.apache.ignite.spi.metric.jmx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.parse;
import static org.apache.ignite.internal.util.IgniteUtils.makeMBeanName;

/**
 * This SPI implementation exports metrics as JMX beans.
 */
public class JmxMetricExporterSpi extends IgniteSpiAdapter implements MetricExporterSpi {
    /** Monitoring registry. */
    private ReadOnlyMetricRegistry mreg;

    /** Metric filter. */
    private @Nullable Predicate<MetricRegistry> filter;

    /** Registered beans. */
    private final List<ObjectName> mBeans = Collections.synchronizedList(new ArrayList<>());

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        mreg.forEach(this::register);

        mreg.addMetricRegistryCreationListener(this::register);
        mreg.addMetricRegistryRemoveListener(this::unregister);
    }

    /** @param grp Metric group to register as JMX bean. */
    private void register(MetricRegistry grp) {
        if (filter != null && !filter.test(grp))
            return;

        if (log.isDebugEnabled())
            log.debug("Found new metric registry [name=" + grp.name() + ']');

        MetricUtils.MetricName n = parse(grp.name());

        try {
            MetricRegistryMBean mregBean = new MetricRegistryMBean(grp);

            ObjectName mbean = U.registerMBean(
                ignite().configuration().getMBeanServer(),
                igniteInstanceName,
                n.root(),
                n.subName(),
                mregBean,
                MetricRegistryMBean.class);

            mBeans.add(mbean);

            if (log.isDebugEnabled())
                log.debug("MetricGroup JMX bean created. " + mbean);
        }
        catch (JMException e) {
            log.error("MBean for " + grp.name() + " can't be created.", e);
        }
    }

    /**
     * Unregister JMX bean for specific metric registry.
     *
     * @param grp Metric group to unregister.
     */
    private void unregister(MetricRegistry grp) {
        if (filter != null && !filter.test(grp))
            return;

        MetricUtils.MetricName n = parse(grp.name());

        try {
            ObjectName mbeanName = makeMBeanName(igniteInstanceName, n.root(), n.subName());

            boolean rmv = mBeans.remove(mbeanName);

            assert rmv : "Failed to remove: " + mbeanName;

            unregBean(ignite, mbeanName);
        }
        catch (MalformedObjectNameException e) {
            log.error("MBean for metric registry '" + n.root() + ',' + n.subName() + "' can't be unregistered.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        Ignite ignite = ignite();

        if (ignite == null)
            return;

        for (ObjectName bean : mBeans)
            unregBean(ignite, bean);
    }

    /**
     * @param ignite Ignite instance.
     * @param bean Bean name to unregister.
     */
    private void unregBean(Ignite ignite, ObjectName bean) {
        MBeanServer jmx = ignite.configuration().getMBeanServer();

        try {
            jmx.unregisterMBean(bean);

            if (log.isDebugEnabled())
                log.debug("Unregistered SPI MBean: " + bean);
        }
        catch (JMException e) {
            log.error("Failed to unregister SPI MBean: " + bean, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setMetricRegistry(ReadOnlyMetricRegistry reg) {
        mreg = reg;
    }

    /** {@inheritDoc} */
    @Override public void setExportFilter(Predicate<MetricRegistry> filter) {
        this.filter = filter;
    }
}
