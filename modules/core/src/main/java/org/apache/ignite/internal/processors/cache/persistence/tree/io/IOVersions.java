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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Registry for IO versions.
 */
public final class IOVersions<V extends PageIO> {
    /** */
    private final PageIO[] vers;

    /** */
    private final PageIO[] ggVers;

    /** */
    private final int type;

    /** */
    private final V latest;

    /**
     * @param vers Versions.
     */
    @SafeVarargs
    public IOVersions(V... vers) {
        assert vers != null;
        assert vers.length > 0;

        this.type = vers[0].getType();

        latest = vers[vers.length - 1];

        List<PageIO> ignVers = new ArrayList<>();
        List<PageIO> ggVers = new ArrayList<>();

        for (PageIO v : vers) {
            if ((short)v.getVersion() > 0)
                ignVers.add(v);
            else
                ggVers.add(v);
        }

        if (ggVers.isEmpty()) {
            this.vers = vers;
            this.ggVers = null;
        }
        else {
            this.vers = ignVers.toArray(new PageIO[]{});
            this.ggVers = ggVers.toArray(new PageIO[]{});

            Arrays.sort(this.ggVers);
        }

        assert checkVersions();
    }

    /**
     * @return Type.
     */
    public int getType() {
        return type;
    }

    /**
     * @return {@code true} If versions are correct.
     */
    private boolean checkVersions() {
        for (int i = 0; i < vers.length; i++) {
            PageIO v = vers[i];

            if (v.getType() != type || v.getVersion() != i + 1)
                return false;
        }

        if (ggVers == null)
            return true;

        for (int i = 0; i < ggVers.length; i++) {
            PageIO v = ggVers[i];

            if (v.getType() != type || (-(short)v.getVersion()) != i + 1)
                return false;
        }

        return true;
    }

    /**
     * @return Latest IO version.
     */
    public V latest() {
        return latest;
    }

    /**
     * @param ver Version.
     * @return IO.
     */
    public V forVersion(int ver) {
        assert ver > 0 && ver <= 65535 : ver;

        short v = (short)ver;

        if (v == 0)
            throw new IllegalStateException("Failed to get page IO instance (page content is corrupted)");

        if (v < 0)
            return (V)ggVers[-v - 1];

        return (V)vers[v - 1];
    }

    /**
     * @param pageAddr Page address.
     * @return IO.
     */
    public V forPage(long pageAddr) {
        int ver = PageIO.getVersion(pageAddr);

        V res = forVersion(ver);

        assert res.getType() == PageIO.getType(pageAddr) : "resType=" + res.getType() +
            ", pageType=" + PageIO.getType(pageAddr);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IOVersions.class, this);
    }
}
