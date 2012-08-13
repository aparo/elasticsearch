/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A filter that filters out deleted documents.
 */
public class NotDeletedFilter extends Filter {

    private final Filter filter;

    public NotDeletedFilter(Filter filter) {
        this.filter = filter;
    }

    public Filter filter() {
        return this.filter;
    }

    @Override
    public String toString() {
        return "NotDeleted(" + filter + ")";
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        DocIdSet docIdSet = filter.getDocIdSet(context, acceptDocs);
        if (docIdSet == null) {
            return null;
        }
        if (!context.reader().hasDeletions()) {
            return docIdSet;
        }
        return new NotDeletedDocIdSet(docIdSet, context);
    }

    static class NotDeletedDocIdSet extends DocIdSet {

        private final DocIdSet innerSet;

        private final AtomicReaderContext reader;

        NotDeletedDocIdSet(DocIdSet innerSet, AtomicReaderContext reader) {
            this.innerSet = innerSet;
            this.reader = reader;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            DocIdSetIterator iterator = innerSet.iterator();
            if (iterator == null) {
                return null;
            }
            return new NotDeletedDocIdSetIterator(iterator, reader);
        }
    }

    static class NotDeletedDocIdSetIterator extends FilteredDocIdSetIterator {

        private final AtomicReaderContext reader;

        NotDeletedDocIdSetIterator(DocIdSetIterator innerIter, AtomicReaderContext reader) {
            super(innerIter);
            this.reader = reader;
        }

        @Override
        protected boolean match(int doc) {
            return reader.reader().getLiveDocs().get(doc);
        }
    }
}
