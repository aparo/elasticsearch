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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.Set;

/**
 * Query that matches no documents.
 */
public final class MatchNoDocsQuery extends Query {

    public static MatchNoDocsQuery INSTANCE = new MatchNoDocsQuery();

    /**
     * Since all instances of this class are equal to each other,
     * we have a constant hash code.
     */
    private static final int HASH_CODE = 12345;

    /**
     * Weight implementation that matches no documents.
     */
    private class MatchNoDocsWeight extends Weight {


        /**
         * Creates a new weight that matches nothing.
         *
         * @param searcher the search to match for
         */
        public MatchNoDocsWeight(final IndexSearcher searcher) {
        }

        @Override
        public String toString() {
            return "weight(" + MatchNoDocsQuery.this + ")";
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new ComplexExplanation(false, 0, "MatchNoDocs matches nothing");
        }

        @Override
        public Query getQuery() {
            return MatchNoDocsQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
             return 0;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
            return null;
        }

    }

    @Override
    public Weight createWeight(final IndexSearcher searcher) {
        return new MatchNoDocsWeight(searcher);
    }

    @Override
    public void extractTerms(final Set<Term> terms) {
    }

    @Override
    public String toString(final String field) {
        return "MatchNoDocsQuery";
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof MatchAllDocsQuery;
    }

    @Override
    public int hashCode() {
        return HASH_CODE;
    }
}