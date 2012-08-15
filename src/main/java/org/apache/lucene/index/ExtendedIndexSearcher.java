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

package org.apache.lucene.index;

import org.apache.lucene.search.IndexSearcher;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ExtendedIndexSearcher extends IndexSearcher {

    public ExtendedIndexSearcher(IndexSearcher searcher) {
        super(searcher.getIndexReader());
        setSimilarity(searcher.getSimilarity());
    }

    public ExtendedIndexSearcher(IndexReader r) {
        super(r);
    }

    public static void gatherSubReaders(List<AtomicReader> allSubReaders, IndexReader reader) {
        List<IndexReaderContext> subReaders = reader.getTopReaderContext().children();
        if (subReaders == null) {
            // Add the reader itself, and do not recurse
            allSubReaders.add((AtomicReader) reader);
        } else {
            for (IndexReaderContext subReader : subReaders) {
                gatherSubReaders(allSubReaders, subReader.reader());
            }
        }
    }

    public AtomicReader[] subReaders() {
        List<AtomicReader> allSubReaders = new ArrayList<AtomicReader>();
        gatherSubReaders(allSubReaders, this.getIndexReader());
        //this.getIndexReader().getTopReaderContext().children().get(0).reader()
        //ReaderUtil.gatherSubReaders(allSubReaders, this.getIndexReader().getTopReaderContext().leaves());
        //TODO non bella conversione
        return allSubReaders.toArray(new AtomicReader[allSubReaders.size()]);
    }

    public int[] docStarts() {
        List<AtomicReaderContext> contexts = this.getIndexReader().getTopReaderContext().leaves();
        int[] result = new int[contexts.size()];
        for (int i = 0; i < contexts.size(); i++) {
            result[i] = contexts.get(i).docBase;
        }
        return result;
    }

    /*
        public int readerIndex(int doc) {
            return DirectoryReader.readerIndex(doc, docStarts(), subReaders().length);
        }
    */
    public AtomicReaderContext contextIndex(int doc) {
        List<AtomicReaderContext> contexts = this.getIndexReader().getTopReaderContext().leaves();

        return contexts.get(ReaderUtil.subIndex(doc, docStarts()));
    }
}
