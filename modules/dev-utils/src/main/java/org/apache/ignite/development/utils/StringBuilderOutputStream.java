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
package org.apache.ignite.development.utils;

import java.io.OutputStream;
import java.nio.charset.Charset;
import org.apache.ignite.internal.util.GridStringBuilder;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Output stream that can be used to print some output to string builder.
 */
public class StringBuilderOutputStream extends OutputStream {
    /** String builder. */
    private final GridStringBuilder sb;

    /** Encoding */
    protected final Charset encoding;

    /**
     * Default constructor.
     */
    public StringBuilderOutputStream() {
        this(new GridStringBuilder(), UTF_8);
    }

    /**
     * Constructor.
     *
     * @param sb String buider.
     */
    public StringBuilderOutputStream(GridStringBuilder sb) {
        this(sb, UTF_8);
    }

    /**
     * Constructor.
     *
     * @param sb String builder.
     * @param encoding Encoding.
     */
    public StringBuilderOutputStream(GridStringBuilder sb, Charset encoding) {
        this.sb = sb;
        this.encoding = encoding;
    }

    /** {@inheritDoc} */
    @Override public void write(int b) {
        sb.a((char)b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte b[]) {
        sb.a(new String(b, encoding));
    }

    /** {@inheritDoc} */
    @Override public void write(byte b[], int off, int len) {
        sb.a(new String(b, off, len, encoding));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return this.sb.toString();
    }
}
