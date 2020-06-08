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

package org.apache.ignite.internal.processors.query.h2;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;

/**
 * SQL function manager.
 */
public class FunctionsManager<T extends Set<String> & Serializable> implements DistributePropertyListener<T> {
    /** Original H2 functions set. */
    private static Map<String, FunctionInfo> origFuncs;

    /** Current H2 functions set. */
    private static Map<String, FunctionInfo> funcs;

    static {
        try {
            Field fldFUNCTIONS = Function.class.getDeclaredField("FUNCTIONS");

            fldFUNCTIONS.setAccessible(true);

            funcs = (Map<String, FunctionInfo>)fldFUNCTIONS.get(Class.class);

            origFuncs = new HashMap<>(funcs);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /** */
    public FunctionsManager() {
        assert Objects.nonNull(funcs);
        assert Objects.nonNull(origFuncs);
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(String s, T oldFuncs, T newFuncs) {
        removeFunctions(newFuncs != null ? newFuncs : DistributedSqlConfiguration.DFLT_DISABLED_FUNCS);
    }

    /** */
    private static void removeFunctions(Set<String> funcNames) {
        funcs.putAll(origFuncs);

        funcs.keySet().removeAll(funcNames);
    }
}