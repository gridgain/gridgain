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

package org.apache.ignite.internal.processors.query.property;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.PropertyMembership;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Binary property.
 */
public class QueryBinaryProperty implements GridQueryProperty {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Property name. */
    private String propName;

    /** */
    private String alias;

    /** Parent property. */
    private QueryBinaryProperty parent;

    /** Result class. */
    private Class<?> type;

    /** Defines where value should be extracted from: cache entry's key or value. */
    private final PropertyMembership membership;

    /** Binary field to speed-up deserialization. */
    private volatile BinaryField field;

    /** Flag indicating that we already tried to take a field. */
    private volatile boolean fieldTaken;

    /** Whether user was warned about missing property. */
    private volatile boolean warned;

    /** */
    private final boolean notNull;

    /** */
    private final Object defaultValue;

    /** */
    private final int precision;

    /** */
    private final int scale;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param propName Property name.
     * @param parent Parent property.
     * @param type Result type.
     * @param membership Whether the property belongs to the cache entry's key, value or both.
     * @param alias Field alias.
     * @param notNull {@code true} if null value is not allowed.
     * @param defaultValue Default value.
     * @param precision Precision.
     * @param scale Scale.
     */
    public QueryBinaryProperty(GridKernalContext ctx, String propName, QueryBinaryProperty parent,
        Class<?> type, PropertyMembership membership, String alias, boolean notNull, Object defaultValue,
        int precision, int scale) {
        this.ctx = ctx;

        log = ctx.log(QueryBinaryProperty.class);

        this.propName = propName;
        this.alias = F.isEmpty(alias) ? propName : alias;
        this.parent = parent;
        this.type = type;
        this.notNull = notNull;
        this.membership = membership;
        this.defaultValue = defaultValue;
        this.precision = precision;
        this.scale = scale;
    }

    /** {@inheritDoc} */
    @Override public Object value(Object key, Object val) throws IgniteCheckedException {
        Object obj;

        if (parent != null) {
            obj = parent.value(key, val);

            if (obj == null)
                return null;

            if (!ctx.cacheObjects().isBinaryObject(obj))
                throw new IgniteCheckedException("Non-binary object received as a result of property extraction " +
                    "[parent=" + parent + ", propName=" + propName + ", obj=" + obj + ']');
        }
        else
            // Extract property value from the Value object if property belongs to both the Key and Value
            // (if membership is PropertyMembership.KEY_VALUE)
            obj = membership == PropertyMembership.KEY ? key : val;

        if (obj instanceof BinaryObject) {
            BinaryObject obj0 = (BinaryObject)obj;

            return fieldValue(obj0);
        }
        else if (obj instanceof BinaryObjectBuilder) {
            BinaryObjectBuilder obj0 = (BinaryObjectBuilder)obj;

            return obj0.getField(propName);
        }
        else
            throw new IgniteCheckedException("Unexpected binary object class [type=" + obj.getClass() + ']');
    }

    /** {@inheritDoc} */
    @Override public void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException {
        if (membership() != PropertyMembership.VALUE)
            setValue(key, val, propVal, key);

        if (membership() != PropertyMembership.KEY)
            setValue(key, val, propVal, val);
    }

    /**
     * Sets this property value for the given object.
     *
     * @param key Key.
     * @param val Value.
     * @param propVal Property value.
     * @param obj The object to set the property of.
     * @throws IgniteCheckedException If failed.
     */
    private void setValue(Object key, Object val, Object propVal, Object obj) throws IgniteCheckedException {
        if (obj == null)
            return;

        Object srcObj = obj;

        if (!(srcObj instanceof BinaryObjectBuilder))
            throw new UnsupportedOperationException("Individual properties can be set for binary builders only");

        if (parent != null)
            obj = parent.value(key, val);

        boolean needsBuild = false;

        if (obj instanceof BinaryObjectExImpl) {
            if (parent == null)
                throw new UnsupportedOperationException("Individual properties can be set for binary builders only");

            needsBuild = true;

            obj = ((BinaryObjectExImpl)obj).toBuilder();
        }

        if (!(obj instanceof BinaryObjectBuilder))
            throw new UnsupportedOperationException("Individual properties can be set for binary builders only");

        setValue0((BinaryObjectBuilder) obj, propName, propVal, type());

        if (needsBuild) {
            obj = ((BinaryObjectBuilder) obj).build();

            assert parent != null;

            // And now let's set this newly constructed object to parent
            setValue0((BinaryObjectBuilder) srcObj, parent.propName, obj, obj.getClass());
        }
    }

    /**
     * @param builder Object builder.
     * @param field Field name.
     * @param val Value to set.
     * @param valType Type of {@code val}.
     * @param <T> Value type.
     */
    private <T> void setValue0(BinaryObjectBuilder builder, String field, Object val, Class<T> valType) {
        //noinspection unchecked
        builder.setField(field, (T)val, valType);
    }

    /**
     * Get binary field for the property.
     *
     * @param obj Target object.
     * @return Binary field.
     */
    private BinaryField binaryField(BinaryObject obj) {
        if (ctx.query().skipFieldLookup())
            return null;

        BinaryField field0 = field;

        if (field0 == null && !fieldTaken) {
            BinaryType type = obj instanceof BinaryObjectEx ? ((BinaryObjectEx)obj).rawType() : obj.type();

            if (type != null) {
                field0 = type.field(propName);

                assert field0 != null;

                field = field0;
            }

            fieldTaken = true;
        }

        return field0;
    }

    /**
     * Gets field value for the given binary object.
     *
     * @param obj Binary object.
     * @return Field value.
     */
    @SuppressWarnings("IfMayBeConditional")
    private Object fieldValue(BinaryObject obj) {
        BinaryField field = binaryField(obj);

        if (field != null)
            return field.value(obj);
        else
            return obj.field(propName);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return alias;
    }

    /** {@inheritDoc} */
    @Override public Class<?> type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public PropertyMembership membership() {
        return membership;
    }

    /** {@inheritDoc} */
    @Override public GridQueryProperty parent() {
        return parent;
    }

    /** {@inheritDoc} */
    @Override public boolean notNull() {
        return notNull;
    }

    /** {@inheritDoc} */
    @Override public Object defaultValue() {
        return defaultValue;
    }

    /** {@inheritDoc} */
    @Override public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override public int scale() {
        return scale;
    }
}
