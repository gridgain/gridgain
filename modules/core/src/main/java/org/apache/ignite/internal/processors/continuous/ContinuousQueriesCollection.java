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

package org.apache.ignite.internal.processors.continuous;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.jetbrains.annotations.NotNull;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Continuous Queries Collection which collected every time from grid */
public class ContinuousQueriesCollection extends AbstractCollection<ContinuousQueryView> {
    /** func for size */
    private final Supplier<Integer> sizeFunc;

    /** local ignite instance name */
    private final String locIgniteName;

    /** */
    public ContinuousQueriesCollection(String locIgniteName, Supplier<Integer> sizeFunc) {
        this.sizeFunc = sizeFunc;
        this.locIgniteName = locIgniteName;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<ContinuousQueryView> iterator() {
        Ignite ignite = Ignition.ignite(locIgniteName);

        return
                ignite.
                        compute(ignite.cluster()).
                        broadcast(new ContinuousQueriesCollectorCallable()).
                        stream().
                        flatMap(c -> c.stream()).
                        iterator();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return sizeFunc.get();
    }

    /** callable for collecting local routine info from grid */
    public static class ContinuousQueriesCollectorCallable implements IgniteCallable<Collection<ContinuousQueryView>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** autowired ignite instance */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Collection<ContinuousQueryView> call() throws Exception {
            return
                    ((IgniteEx)ignite).
                            context().
                            continuous().
                            getLocalContinuousQueryRoutines().
                            stream().
                            map(
                                    r -> new ContinuousQueryView(r.getKey(), r.getValue(), ignite.cluster().localNode().consistentId().toString())
                            ).
                            collect(Collectors.toSet());
        }
    }
}
