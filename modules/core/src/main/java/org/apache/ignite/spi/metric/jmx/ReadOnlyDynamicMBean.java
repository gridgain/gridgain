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

import java.util.List;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import org.apache.ignite.IgniteException;

/**
 * Base class for read only {@link DynamicMBean} implementations.
 */
public abstract class ReadOnlyDynamicMBean implements DynamicMBean {
    /** {@inheritDoc} */
    @Override public void setAttribute(Attribute attr) {
        throw new UnsupportedOperationException("setAttribute is not supported.");
    }

    /** {@inheritDoc} */
    @Override public AttributeList setAttributes(AttributeList attrs) {
        throw new UnsupportedOperationException("setAttributes is not supported.");
    }

    /** {@inheritDoc} */
    @Override public Object invoke(String actionName, Object[] params, String[] signature)
        throws MBeanException, ReflectionException {
        try {
            if ("getAttribute".equals(actionName))
                return getAttribute((String)params[0]);
            else if ("invoke".equals(actionName))
                return invoke((String)params[0], (Object[])params[1], (String[])params[2]);
        }
        catch (AttributeNotFoundException e) {
            throw new MBeanException(e);
        }

        throw new UnsupportedOperationException("invoke is not supported.");
    }

    /** {@inheritDoc} */
    @Override public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();
        List<Attribute> attrList = list.asList();

        try {
            for (String attr : attributes) {
                Object val = getAttribute(attr);

                if (val instanceof Attribute)
                    attrList.add((Attribute)val);
                else
                    attrList.add(new Attribute(attr, val));
            }

            return list;
        }
        catch (MBeanException | ReflectionException | AttributeNotFoundException e) {
            throw new IgniteException(e);
        }
    }
}
