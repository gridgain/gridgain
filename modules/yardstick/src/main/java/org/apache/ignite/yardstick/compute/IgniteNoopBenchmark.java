package org.apache.ignite.yardstick.compute;

import java.util.Map;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;

public class IgniteNoopBenchmark extends IgniteAbstractBenchmark {
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Thread.sleep(100);

        return true;
    }
}
