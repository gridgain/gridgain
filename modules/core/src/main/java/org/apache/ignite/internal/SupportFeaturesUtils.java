/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal;

import org.apache.ignite.IgniteSystemProperties;

/**
 * Only for internal usage.
 */
public class SupportFeaturesUtils {
    /**
     * Undocumented experimental internal API that must not be touched by regular users.
     */
    public static final String IGNITE_DISTRIBUTED_META_STORAGE_FEATURE = "IGNITE_DISTRIBUTED_META_STORAGE_FEATURE";
    /**
     * Flag to turn on and off support of baseline topology for in-memory caches feature.
     *
     * For internal use only, must not be exposed to end users.
     */
    public static final String IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE =
        "IGNITE_BASELINE_FOR_IN_MEMORY_CACHES_FEATURE";
    /**
     * Flag to enable baseline auto-adjust feature.
     */
    public static final String IGNITE_BASELINE_AUTO_ADJUST_FEATURE = "IGNITE_BASELINE_AUTO_ADJUST_FEATURE";

    /**
     * @param featureName System property feature name.
     * @return {@code true} If given feature is enabled.
     */
    public static boolean isFeatureEnabled(String featureName) {
        return IgniteSystemProperties.getBoolean(featureName, false);
    }
}
