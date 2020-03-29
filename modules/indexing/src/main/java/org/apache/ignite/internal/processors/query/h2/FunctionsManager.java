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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;

/**
 * SQL function manager.
 */
@SuppressWarnings("unchecked")
public class FunctionsManager {
    /** Original H2 functions set. */
    private static HashMap<String, FunctionInfo> origFuncs;

    /** Current H2 functions set. */
    private static HashMap<String, FunctionInfo> funcs;

    static {
        try {
            Field fldFUNCTIONS = Function.class.getDeclaredField("FUNCTIONS");

            fldFUNCTIONS.setAccessible(true);

            funcs = (HashMap<String, FunctionInfo>)fldFUNCTIONS.get(Class.class);

            origFuncs = new HashMap<>(funcs);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /** Logger. */
    private final IgniteLogger log;

    /**
     *
     */
    public FunctionsManager(IgniteH2Indexing idx,
        DistributedSqlConfiguration distSqlCfg) {
        assert Objects.nonNull(funcs);
        assert Objects.nonNull(origFuncs);

        log = idx.kernalContext().log(FunctionsManager.class);

        distSqlCfg.listenDisabledFunctions(this::updateDisabledFunctions);
    }

    /**
     *
     */
    private void updateDisabledFunctions(String s, HashSet<String> oldFuncs, HashSet<String> newFuncs) {
        if (newFuncs != null)
            removeFunctions(newFuncs);
        else
            removeFunctions(DistributedSqlConfiguration.DFLT_DISABLED_FUNCS);
    }

    /**
     *
     */
    private static void removeFunctions(Set<String> funcNames) {
        funcs.putAll(origFuncs);

        funcs.keySet().removeAll(funcNames);
    }
}