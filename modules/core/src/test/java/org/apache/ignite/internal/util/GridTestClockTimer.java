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

package org.apache.ignite.internal.util;

import java.util.function.LongSupplier;

/**
 * Clock timer for tests.
 */
public class GridTestClockTimer implements Runnable {
    /** Default time supplier. */
    public static final LongSupplier DFLT_TIME_SUPPLIER = System::currentTimeMillis;

    /** Current time supplier. */
    private static volatile LongSupplier timeSupplier = DFLT_TIME_SUPPLIER;

    /** Mutex to avoid races between time updates. */
    private static final Object mux = new Object();

    /**
     * Constructor.
     */
    public GridTestClockTimer() {
        synchronized (IgniteUtils.mux) {
            assert IgniteUtils.gridCnt == 0 : IgniteUtils.gridCnt;

            IgniteUtils.gridCnt++; // To prevent one more timer thread start from IgniteUtils.onGridStart.
        }
    }

    /**
     * @return {@code True} if need start test time.
     */
    public static boolean startTestTimer() {
        synchronized (IgniteUtils.mux) {
            return IgniteUtils.gridCnt == 0;
        }
    }

    /**
     * Sets new time supplier.
     *
     * @param timeSupplier Time supplier.
     */
    public static void timeSupplier(LongSupplier timeSupplier) {
        GridTestClockTimer.timeSupplier = timeSupplier;
    }

    /**
     * Updates current time with value supplied by time supplier.
     */
    public static void update() {
        synchronized (mux) {
            IgniteUtils.curTimeMillis = timeSupplier.getAsLong();
        }
    }

    /** {@inheritDoc} */
    @Override public void run() {
        while (true) {
            update();

            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                break;
            }
        }
    }
}