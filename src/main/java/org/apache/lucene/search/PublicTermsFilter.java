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

package org.apache.lucene.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 */
// LUCENE MONITOR: Against TermsFilter
public class PublicTermsFilter extends Filter {

    Set<Term> terms = new TreeSet<Term>();

    /**
     * Adds a term to the list of acceptable terms
     *
     * @param term
     */
    public void addTerm(Term term) {
        terms.add(term);
    }

    public Set<Term> getTerms() {
        return terms;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if ((obj == null) || (obj.getClass() != this.getClass()))
            return false;
        PublicTermsFilter test = (PublicTermsFilter) obj;
        return (terms == test.terms ||
                (terms != null && terms.equals(test.terms)));
    }

    @Override
    public int hashCode() {
        int hash = 9;
        for (Iterator<Term> iter = terms.iterator(); iter.hasNext(); ) {
            Term term = iter.next();
            hash = 31 * hash + term.hashCode();
        }
        return hash;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext atomicReaderContext, Bits bits) throws IOException {
        FixedBitSet result = null;
        int doc;
        for (Term term : terms) {
            DocsEnum td = MultiFields.getTermDocsEnum(atomicReaderContext.reader(), MultiFields.getLiveDocs(atomicReaderContext.reader()), term.field(), term.bytes(), DocsEnum.FLAG_FREQS);
            if (td != null) {
                if (result == null) {
                    result = new FixedBitSet(atomicReaderContext.reader().maxDoc());
                }
                while ((doc = td.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
                    result.set(doc);
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Term term : terms) {
            if (builder.length() > 0) {
                builder.append(' ');
            }
            builder.append(term);
        }
        return builder.toString();
    }

}
