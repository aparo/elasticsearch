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

package org.elasticsearch.common.lucene.all;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.apache.lucene.analysis.payloads.PayloadHelper.decodeFloat;

/**
 * A term query that takes all payload boost values into account.
 *
 *
 */
public class AllTermQuery extends SpanTermQuery {

    private boolean includeSpanScore;

    public AllTermQuery(Term term) {
        this(term, true);
    }

    public AllTermQuery(Term term, boolean includeSpanScore) {
        super(term);
        this.includeSpanScore = includeSpanScore;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        return new AllTermWeight(this, searcher);
    }

    protected class AllTermWeight extends SpanWeight {
        protected Map<Term,TermContext> termContexts;

        public AllTermWeight(AllTermQuery query, IndexSearcher searcher) throws IOException {
            super(query, searcher);
            termContexts = new HashMap<Term,TermContext>();
            TreeSet<Term> terms = new TreeSet<Term>();
            query.extractTerms(terms);
            final IndexReaderContext context = searcher.getTopReaderContext();
            final TermStatistics termStats[] = new TermStatistics[terms.size()];
            int i = 0;
            for (Term term : terms) {
              TermContext state = TermContext.build(context, term, true);
              termStats[i] = searcher.termStatistics(term, state);
              termContexts.put(term, state);
              i++;
            }

        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {

            return new AllTermSpanScorer((TermSpans) query.getSpans(context, acceptDocs, termContexts), this,
                    similarity.sloppySimScorer(stats, context));
        }

        protected class AllTermSpanScorer extends SpanScorer {
            // TODO: is this the best way to allocate this?
            protected BytesRef payload = new BytesRef();
            protected DocsAndPositionsEnum positions;
            protected float payloadScore;
            protected int payloadsSeen;

            public AllTermSpanScorer(TermSpans spans, Weight weight, Similarity.SloppySimScorer docScorer) throws IOException {
                super(spans, weight, docScorer);
                positions = spans.getPostings();
            }

            @Override
            protected boolean setFreqCurrentDoc() throws IOException {
                if (!more) {
                    return false;
                }
                doc = spans.doc();
                freq = 0.0f;
                payloadScore = 0;
                payloadsSeen = 0;
                while (more && doc == spans.doc()) {
                    int matchLength = spans.end() - spans.start();

                    freq += this.docScorer.computeSlopFactor(matchLength);
                    processPayload();

                    more = spans.next();// this moves positions to the next match in this
                    // document
                }
                return more || (freq != 0);
            }

            protected void processPayload() throws IOException {
                if (positions.hasPayload()) {
                    payload = positions.getPayload();
                    payloadScore += decodeFloat(payload.bytes);
                    payloadsSeen++;

                } else {
                    // zero out the payload?
                }
            }

            /**
             * @return {@link #getSpanScore()} * {@link #getPayloadScore()}
             * @throws IOException
             */
            @Override
            public float score() throws IOException {
                return includeSpanScore ? getSpanScore() * getPayloadScore() : getPayloadScore();
            }

            /**
             * Returns the SpanScorer score only.
             * <p/>
             * Should not be overridden without good cause!
             *
             * @return the score for just the Span part w/o the payload
             * @throws IOException
             * @see #score()
             */
            protected float getSpanScore() throws IOException {
                return super.score();
            }

            /**
             * The score for the payload
             */
            protected float getPayloadScore() {
                return payloadsSeen > 0 ? (payloadScore / payloadsSeen) : 1;
            }
            /*
            public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
                ComplexExplanation result = new ComplexExplanation();
                //TODO FIX COMMENTED OUT
                //Explanation nonPayloadExpl = super.explain(doc);
                //result.addDetail(nonPayloadExpl);

                // QUESTION: Is there a way to avoid this skipTo call? We need to know
                // whether to load the payload or not
                Explanation payloadBoost = new Explanation();
                result.addDetail(payloadBoost);

                float payloadScore = getPayloadScore();
                payloadBoost.setValue(payloadScore);
                // GSI: I suppose we could toString the payload, but I don't think that
                // would be a good idea
                //payloadBoost.setDescription("allPayload(...)");
                //result.setValue(nonPayloadExpl.getValue() * payloadScore);
                //result.setDescription("btq, product of:");
                //result.setMatch(nonPayloadExpl.getValue() == 0 ? Boolean.FALSE : Boolean.TRUE); // LUCENE-1303
                return result;
            } */

        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (includeSpanScore ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        AllTermQuery other = (AllTermQuery) obj;
        if (includeSpanScore != other.includeSpanScore)
            return false;
        return true;
    }

}
