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
import java.util.stream.Collectors;

/** Continuous Queries Collection which collected every time from grid */
public class ContinuousQueriesCollection extends AbstractCollection<ContinuousQueryView> {
    /** init collections only for size */
    private final Collection<?> coll;

    /** local ignite instance name */
    private final String name;

    /** */
    public ContinuousQueriesCollection(Collection<?> coll, String name) {
        this.coll = coll;
        this.name = name;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<ContinuousQueryView> iterator() {
        Ignite ignite = Ignition.ignite(name);

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
        return coll.size();
    }

    /** callable for collecting local routine info from grid */
    public static class ContinuousQueriesCollectorCallable implements IgniteCallable<Collection<ContinuousQueryView>> {
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
                                    r -> new ContinuousQueryView(r.getKey(), r.getValue())
                            ).
                            collect(Collectors.toSet());
        }
    }
}
