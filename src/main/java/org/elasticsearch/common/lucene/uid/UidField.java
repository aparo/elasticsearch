/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.io.Reader;

/**
 *
 */
public class UidField extends Field {

    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final IndexReader reader;

        public DocIdAndVersion(int docId, long version, IndexReader reader) {
            this.docId = docId;
            this.version = version;
            this.reader = reader;
        }
    }

    // this works fine for nested docs since they don't have the payload which has the version
    // so we iterate till we find the one with the payload
    public static DocIdAndVersion loadDocIdAndVersion(IndexReader reader, Term term) {
        int docId = Lucene.NO_DOC;
        DocsAndPositionsEnum uid = null;
        try {
            uid = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader), term.field(), term.bytes());
            if (uid==null) {
                return null; // no doc
            }
            // Note, only master docs uid have version payload, so we can use that info to not
            // take them into account
            int doc;
            while((doc = uid.nextDoc()) != DocsAndPositionsEnum.NO_MORE_DOCS) {
                docId = uid.docID();
                uid.nextPosition();
                if (!uid.hasPayload()) {
                    continue;
                }
                BytesRef payload = uid.getPayload();
                if (payload.length < 8) {
                    continue;
                }
                return new DocIdAndVersion(docId, Numbers.bytesToLong(payload.bytes), reader);
             }
            return new DocIdAndVersion(docId, -2, reader);
        } catch (Exception e) {
            return new DocIdAndVersion(docId, -2, reader);
        }
    }

    /**
     * Load the version for the uid from the reader, returning -1 if no doc exists, or -2 if
     * no version is available (for backward comp.)
     */
    public static long loadVersion(IndexReader reader, Term term) {
        DocsAndPositionsEnum uid = null;
        try {
            uid = MultiFields.getTermPositionsEnum(reader, MultiFields.getLiveDocs(reader), term.field(), term.bytes());
            if (uid==null) {
                return -1;
            }
            // Note, only master docs uid have version payload, so we can use that info to not
            // take them into account

            int doc;
            while((doc = uid.nextDoc()) != DocsAndPositionsEnum.NO_MORE_DOCS) {
                uid.nextPosition();
                if (!uid.hasPayload()) {
                    continue;
                }
                BytesRef payload = uid.getPayload();
                if (payload.length < 8) {
                    continue;
                }
                return Numbers.bytesToLong(payload.bytes);
            }
            return -2;
        } catch (Exception e) {
            return -2;
        }
    }

    private String uid;

    private long version;

    public UidField(String name, String uid, long version) {
        super(name, new FieldType());
        this.uid = uid;
        this.version = version;
        FieldType ft = (FieldType)this.fieldType();
        ft.setIndexed(true);
        ft.setStored(true);
        ft.setTokenized(true);
        ft.setOmitNorms(true);
        ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        this.setTokenStream(new UidPayloadTokenStream(this));
    }

    public String uid() {
        return this.uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String stringValue() {
        return uid;
    }

    @Override
    public Reader readerValue() {
        return null;
    }

    public long version() {
        return this.version;
    }

    public void version(long version) {
        this.version = version;
    }

    @Override
    public TokenStream tokenStreamValue() {
        return tokenStream;
    }

    public static final class UidPayloadTokenStream extends TokenStream {

        private final PayloadAttribute payloadAttribute = addAttribute(PayloadAttribute.class);
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        private final UidField field;

        private boolean added = false;

        public UidPayloadTokenStream(UidField field) {
            this.field = field;
        }

        @Override
        public void reset() throws IOException {
            added = false;
        }

        @Override
        public final boolean incrementToken() throws IOException {
            if (added) {
                return false;
            }
            termAtt.setLength(0);
            termAtt.append(field.uid);
            payloadAttribute.setPayload(new BytesRef(Numbers.longToBytes(field.version())));
            added = true;
            return true;
        }
    }
}
