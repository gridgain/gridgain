/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class to print only limited number of warnings.
 */
public class WarningsGroup {
    /** */
    private final IgniteLogger log;

    /** */
    private final int warningsLimit;

    /** */
    private final String title;

    /** */
    private final List<String> messages = new ArrayList<>();

    /** */
    private int warningsTotal = 0;

    /**
     * @param log Target logger.
     * @param warningsLimit Warnings limit.
     */
    public WarningsGroup(String title, IgniteLogger log, int warningsLimit) {
        this.title = title;

        this.log = log;

        this.warningsLimit = warningsLimit;
    }

    /**
     * @param msg Warning message.
     * @return {@code true} if message is added to list.
     */
    public boolean add(String msg) {
        boolean added = false;

        if (canAddMessage()) {
            messages.add(msg);

            added = true;
        }

        warningsTotal++;

        return added;
    }

    /**
     * @return {@code true} if messages list size less than limit.
     */
    public boolean canAddMessage() {
        return warningsTotal < warningsLimit;
    }

    /**
     * Increase total number of warnings.
     */
    public void incTotal() {
        warningsTotal++;
    }

    /**
     * Print warnings block title and messages.
     */
    public void printToLog() {
        if (warningsTotal > 0) {
            U.warn(log, String.format(title, warningsLimit, warningsTotal));

            for (String message : messages)
                U.warn(log, message);
        }
    }
}