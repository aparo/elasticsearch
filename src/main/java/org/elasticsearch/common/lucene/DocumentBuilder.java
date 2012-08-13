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

package org.elasticsearch.common.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.lucene.uid.UidField;

/**
 *
 */
public class DocumentBuilder {

    public static final Document EMPTY = new Document();

    public static DocumentBuilder doc() {
        return new DocumentBuilder();
    }

    public static IndexableField uidField(String value) {
        return uidField(value, 0);
    }

    public static IndexableField uidField(String value, long version) {
        return new UidField("_uid", value, version);
    }

    public static FieldBuilder field(String name, String value) {
        return field(name, value, true, true);
    }

    public static FieldBuilder field(String name, String value, boolean store, boolean index) {
        return new FieldBuilder(name, value, store, index);
    }

    public static FieldBuilder field(String name, String value, boolean store, boolean index, boolean storeTermVectors, boolean storeTermVectorOffsets, boolean storeTermVectorPositions) {
        return new FieldBuilder(name, value, store, index, storeTermVectors, storeTermVectorOffsets, storeTermVectorPositions);
    }

    public static FieldBuilder field(String name, byte[] value, boolean store) {
        return new FieldBuilder(name, value, store);
    }

    public static FieldBuilder field(String name, byte[] value, int offset, int length, boolean store) {
        return new FieldBuilder(name, value, offset, length, store);
    }

    private final Document document;

    private DocumentBuilder() {
        this.document = new Document();
    }

    public DocumentBuilder boost(float boost) {
        //FIX REMOVED IN LUCENE4
        //document.setBoost(boost);
        return this;
    }

    public DocumentBuilder add(IndexableField field) {
        document.add(field);
        return this;
    }

    public DocumentBuilder add(FieldBuilder fieldBuilder) {
        document.add(fieldBuilder.build());
        return this;
    }

    public Document build() {
        return document;
    }
}
