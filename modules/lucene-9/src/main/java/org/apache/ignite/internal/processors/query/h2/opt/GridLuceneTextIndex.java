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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.LuceneIndex;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.gridgain.internal.h2.util.JdbcUtils;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;

/**
 * Lucene index implementation that supports only TEXT queries.
 */
public class GridLuceneTextIndex implements LuceneIndex {
    /** Field name for string representation of value. */
    public static final String VAL_STR_FIELD_NAME = "_gg_val_str__";

    /** Field name for value version. */
    public static final String VER_FIELD_NAME = "_gg_ver__";

    /** Field name for value expiration time. */
    public static final String EXPIRATION_TIME_FIELD_NAME = "_gg_expires__";

    /** */
    private GridKernalContext ctx;

    /** */
    private String cacheName;

    /** */
    private GridQueryTypeDescriptor type;

    /** */
    private IndexWriter writer;

    /** */
    private String[] idxdFields;

    /** */
    private AtomicLong updateCntr = new GridAtomicLong();

    /** */
    private GridLuceneDirectory dir;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param type Type descriptor.
     */
    public GridLuceneTextIndex(GridKernalContext ctx, @Nullable String cacheName,
        GridQueryTypeDescriptor type) {
        this.ctx = ctx;
        this.cacheName = cacheName;
        this.type = type;

        dir = new GridLuceneDirectory(new GridUnsafeMemory(0));

        try {
            writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()));
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        GridQueryIndexDescriptor idx = type.textIndex();

        if (idx != null) {
            Collection<String> fields = idx.fields();

            idxdFields = new String[fields.size() + 1];

            fields.toArray(idxdFields);
        }
        else {
            assert type.valueTextIndex() || type.valueClass() == String.class;

            idxdFields = new String[1];
        }

        idxdFields[idxdFields.length - 1] = VAL_STR_FIELD_NAME;
    }

    /**
     * @return Cache object context.
     */
    private CacheObjectContext objectContext() {
        if (ctx == null)
            return null;

        return ctx.cache().internalCache(cacheName).context().cacheObjectContext();
    }

    /** {@inheritDoc} */
    @Override public void store(CacheObject k, CacheObject v, GridCacheVersion ver, long expires)
        throws IgniteCheckedException {
        CacheObjectContext coctx = objectContext();

        Object key = k.isPlatformType() ? k.value(coctx, false) : k;
        Object val = v.isPlatformType() ? v.value(coctx, false) : v;

        Document doc = new Document();

        boolean stringsFound = false;

        if (type.valueTextIndex() || type.valueClass() == String.class) {
            doc.add(new TextField(VAL_STR_FIELD_NAME, val.toString(), Field.Store.YES));

            stringsFound = true;
        }

        for (int i = 0, last = idxdFields.length - 1; i < last; i++) {
            Object fieldVal = type.value(idxdFields[i], key, val);

            if (fieldVal != null) {
                doc.add(new TextField(idxdFields[i], fieldVal.toString(), Field.Store.YES));

                stringsFound = true;
            }
        }

        BytesRef keyByteRef = new BytesRef(k.valueBytes(coctx));

        try {
            final Term term = new Term(KEY_FIELD_NAME, keyByteRef);

            if (!stringsFound) {
                writer.deleteDocuments(term);

                return; // We did not find any strings to be indexed, will not store data at all.
            }

            doc.add(new StringField(KEY_FIELD_NAME, keyByteRef, Field.Store.YES));

            if (type.valueClass() != String.class)
                doc.add(new StoredField(VAL_FIELD_NAME, v.valueBytes(coctx)));

            doc.add(new StoredField(VER_FIELD_NAME, ver.toString().getBytes()));

            doc.add(new LongPoint(EXPIRATION_TIME_FIELD_NAME, expires));

            // Next implies remove than add atomically operation.
            writer.updateDocument(term, doc);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            updateCntr.incrementAndGet();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(CacheObject key) throws IgniteCheckedException {
        try {
            writer.deleteDocuments(new Term(KEY_FIELD_NAME,
                new BytesRef(key.valueBytes(objectContext()))));
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            updateCntr.incrementAndGet();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> textQuery(String qry,
        IndexingQueryFilter filters) throws IgniteCheckedException {
        IndexReader reader;

        try {
            long updates = updateCntr.get();

            if (updates != 0) {
                writer.commit();

                updateCntr.addAndGet(-updates);
            }

            //We can cache reader\searcher and change this to 'openIfChanged'
            reader = DirectoryReader.open(writer);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        IndexSearcher searcher;

        TopDocs docs;

        try {
            searcher = new IndexSearcher(reader);

            MultiFieldQueryParser parser = new MultiFieldQueryParser(idxdFields,
                writer.getAnalyzer());

            // Filter expired items.
            Query filter = LongPoint.newRangeQuery(EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(), Long.MAX_VALUE);

            BooleanQuery query = new BooleanQuery.Builder()
                .add(parser.parse(qry), BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER)
                .build();

            docs = searcher.search(query, Integer.MAX_VALUE);
        }
        catch (Exception e) {
            U.closeQuiet(reader);

            throw new IgniteCheckedException(e);
        }

        IndexingQueryCacheFilter fltr = null;

        if (filters != null)
            fltr = filters.forCache(cacheName);

        return new It<>(reader, searcher, docs.scoreDocs, fltr);
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> vectorQuery(String field,
        float[] qryVector, int k, float threshold, IndexingQueryFilter filters) throws IgniteCheckedException {
        throw new IllegalStateException("To use vector query feature, enable gridgain-vector-query module" +
            " (requires Enterprise or Ultimate Edition)");
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(writer);
        U.close(dir, ctx.log(GridLuceneTextIndex.class));
    }

    /**
     * Key-value iterator over fulltext search result.
     */
    private class It<K, V> extends GridCloseableIteratorAdapter<IgniteBiTuple<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IndexReader reader;

        /** */
        private final IndexSearcher searcher;

        /** */
        private final ScoreDoc[] docs;

        /** */
        private final IndexingQueryCacheFilter filters;

        /** */
        private int idx;

        /** */
        private IgniteBiTuple<K, V> curr;

        /** */
        private CacheObjectContext coctx;

        /**
         * Constructor.
         *
         * @param reader Reader.
         * @param searcher Searcher.
         * @param docs Docs.
         * @param filters Filters over result.
         * @throws IgniteCheckedException if failed.
         */
        private It(IndexReader reader, IndexSearcher searcher, ScoreDoc[] docs, IndexingQueryCacheFilter filters)
            throws IgniteCheckedException {
            this.reader = reader;
            this.searcher = searcher;
            this.docs = docs;
            this.filters = filters;

            coctx = objectContext();

            findNext();
        }

        /**
         * @param bytes Bytes.
         * @param ldr Class loader.
         * @return Object.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private <Z> Z unmarshall(byte[] bytes, ClassLoader ldr) throws IgniteCheckedException {
            if (coctx == null) // For tests.
                return (Z)JdbcUtils.deserialize(bytes, null);

            return (Z)coctx.kernalContext().cacheObjects().unmarshal(coctx, bytes, ldr);
        }

        /**
         * Finds next element.
         *
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void findNext() throws IgniteCheckedException {
            curr = null;

            while (idx < docs.length) {
                Document doc;

                try {
                    doc = searcher.doc(docs[idx++].doc);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                ClassLoader ldr = null;

                if (ctx != null && ctx.deploy().enabled())
                    ldr = ctx.cache().internalCache(cacheName).context().deploy().globalLoader();

                K k = unmarshall(doc.getBinaryValue(KEY_FIELD_NAME).bytes, ldr);

                if (filters != null && !filters.apply(k))
                    continue;

                V v = type.valueClass() == String.class ?
                    (V)doc.get(VAL_STR_FIELD_NAME) :
                    this.<V>unmarshall(doc.getBinaryValue(VAL_FIELD_NAME).bytes, ldr);

                assert v != null;

                curr = new IgniteBiTuple<>(k, v);

                break;
            }
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<K, V> onNext() throws IgniteCheckedException {
            IgniteBiTuple<K, V> res = curr;

            findNext();

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return curr != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            U.closeQuiet(reader);
        }
    }
}
