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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;

/**
 *
 */
public abstract class AbstractFieldMapper<T> implements FieldMapper<T>, Mapper {

    public static class Defaults {
        public static final boolean INDEX = true;
        public static final boolean STORE_TERM_VECTOR = false;
        public static final boolean STORE_TERM_VECTOR_OFFSETS = false;
        public static final boolean STORE_TERM_VECTOR_POSITIONS = false;
        public static final boolean STORE = false;
        public static final boolean TOKENIZE = true;
        public static final float BOOST = 1.0f;
        public static final boolean OMIT_NORMS = false;
        public static final FieldInfo.IndexOptions INDEX_OPTIONS = FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    }

    public abstract static class OpenBuilder<T extends Builder, Y extends AbstractFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        protected OpenBuilder(String name) {
            super(name);
        }

        @Override
        public T index(boolean index) {
            return super.index(index);
        }

        @Override
        public T store(boolean store) {
            return super.store(store);
        }

        @Override
        public T tokenize(boolean store) {
            return super.tokenize(store);
        }

        @Override
        public T storeTermVectors(boolean storeTermVectors) {
            return super.storeTermVectors(storeTermVectors);
        }

        @Override
        public T storeTermVectorOffsets(boolean storeTermVectorOffsets) {
            return super.storeTermVectorOffsets(storeTermVectorOffsets);
        }

        @Override
        public T storeTermVectorPositions(boolean storeTermVectorPositions) {
            return super.storeTermVectorPositions(storeTermVectorPositions);
        }

        @Override
        public T boost(float boost) {
            return super.boost(boost);
        }

        @Override
        public T omitNorms(boolean omitNorms) {
            return super.omitNorms(omitNorms);
        }        
        
        @Override
        public T indexOptions(FieldInfo.IndexOptions indexOptions) {
            return super.indexOptions(indexOptions);
        }

        @Override
        public T indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override
        public T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            return super.indexAnalyzer(indexAnalyzer);
        }

        @Override
        public T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            return super.searchAnalyzer(searchAnalyzer);
        }
    }

    public abstract static class Builder<T extends Builder, Y extends AbstractFieldMapper> extends Mapper.Builder<T, Y> {

        protected boolean index = Defaults.INDEX;

        protected boolean store = Defaults.STORE;

        protected boolean tokenize = Defaults.TOKENIZE;

        protected boolean storeTermVectors = Defaults.STORE_TERM_VECTOR;

        protected boolean storeTermVectorOffsets = Defaults.STORE_TERM_VECTOR_OFFSETS;

        protected boolean storeTermVectorPositions = Defaults.STORE_TERM_VECTOR_POSITIONS;

        protected float boost = Defaults.BOOST;

        protected boolean omitNorms = Defaults.OMIT_NORMS;

        protected FieldInfo.IndexOptions indexOptions = Defaults.INDEX_OPTIONS;

        protected String indexName;

        protected NamedAnalyzer indexAnalyzer;

        protected NamedAnalyzer searchAnalyzer;

        protected Boolean includeInAll;

        protected Builder(String name) {
            super(name);
        }

        protected T index(boolean index) {
            this.index = index;
            return builder;
        }

        protected T store(boolean store) {
            this.store = store;
            return builder;
        }

        protected T tokenize(boolean tokenize) {
            this.tokenize = tokenize;
            return builder;
        }

        protected T storeTermVectors(boolean storeTermVectors) {
            this.storeTermVectors = storeTermVectors;
            return builder;
        }

        protected T storeTermVectorOffsets(boolean storeTermVectorOffsets) {
            this.storeTermVectorOffsets = storeTermVectorOffsets;
            return builder;
        }

        protected T storeTermVectorPositions(boolean storeTermVectorPositions) {
            this.storeTermVectorPositions = storeTermVectorPositions;
            return builder;
        }

        protected T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        protected T omitNorms(boolean omitNorms) {
            this.omitNorms = omitNorms;
            return builder;
        }

        protected T indexOptions(FieldInfo.IndexOptions indexOptions) {
            this.indexOptions = indexOptions;
            return builder;
        }

        protected T indexName(String indexName) {
            this.indexName = indexName;
            return builder;
        }

        protected T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            if (this.searchAnalyzer == null) {
                this.searchAnalyzer = indexAnalyzer;
            }
            return builder;
        }

        protected T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return builder;
        }

        protected T includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return builder;
        }

        protected Names buildNames(BuilderContext context) {
            return new Names(name, buildIndexName(context), indexName == null ? name : indexName, buildFullName(context), context.path().sourcePath());
        }

        protected String buildIndexName(BuilderContext context) {
            String actualIndexName = indexName == null ? name : indexName;
            return context.path().pathAsText(actualIndexName);
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().fullPathAsText(name);
        }
    }

    protected final Names names;

    protected final Field.Index index;

    protected final Field.Store store;

    protected final Field.TermVector termVector;

    protected float boost;

    protected final boolean omitNorms;

    protected final boolean omitTermFreqAndPositions;
    protected final FieldInfo.IndexOptions indexOptions;

    protected final NamedAnalyzer indexAnalyzer;

    protected final NamedAnalyzer searchAnalyzer;

    protected boolean index;
    protected boolean tokenize;
    protected boolean store;
    protected boolean storeTermVectors;
    protected boolean storeTermVectorOffsets;
    protected boolean storeTermVectorPositions;
    protected boolean omitNorms;
    protected FieldInfo.IndexOptions indexOptions;

    protected AbstractFieldMapper(Names names, boolean index, boolean tokenize, boolean store,
                                  boolean storeTermVectors, boolean storeTermVectorOffsets, boolean storeTermVectorPositions,
                                  float boost, boolean omitNorms, FieldInfo.IndexOptions indexOptions,
                                  NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        this.names = names;
        this.index = index;
        this.tokenize = tokenize;
        this.store = store;
        this.storeTermVectors=storeTermVectors;
        this.storeTermVectorOffsets=storeTermVectorOffsets;
        this.storeTermVectorPositions=storeTermVectorPositions;
        this.omitNorms=omitNorms;
        this.indexOptions=indexOptions;
        this.boost = boost;
        // automatically set to keyword analyzer if its indexed and not analyzed
        if (indexAnalyzer == null && index && !tokenize) {
            this.indexAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.indexAnalyzer = indexAnalyzer;
        }
        // automatically set to keyword analyzer if its indexed and not analyzed
        if (searchAnalyzer == null && index && !tokenize) {
            this.searchAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.searchAnalyzer = searchAnalyzer;
        }
    }

    public FieldType getFieldType(){
        FieldType fieldType = Lucene.getDefaultFieldType();
        fieldType.setIndexed(index);
        fieldType.setTokenized(tokenize);
        fieldType.setStored(store);
        fieldType.setStoreTermVectors(storeTermVectors);
        fieldType.setStoreTermVectorOffsets(storeTermVectorOffsets);
        fieldType.setStoreTermVectorPositions(storeTermVectorPositions);
        fieldType.setOmitNorms(omitNorms);
        fieldType.setIndexOptions(indexOptions);
        return fieldType;
    }

    @Override
    public String name() {
        return names.name();
    }

    @Override
    public Names names() {
        return this.names;
    }

    @Override
    public boolean index() {
        return this.index;
    }

    @Override
    public boolean store() {
        return this.store;
    }

    @Override
    public boolean stored() {
        return this.store;
    }

    @Override
    public boolean tokenized() {
        return this.tokenize;
    }

    @Override
    public boolean indexed() {
        return this.index;
    }

    @Override
    public FieldInfo.IndexOptions indexOptions() {
        return this.indexOptions;
    }

    @Override
    public boolean storeTermVectors() {
        return this.storeTermVectors;
    }

    @Override
    public boolean storeTermVectorOffsets() {
        return this.storeTermVectorOffsets;
    }

    @Override
    public boolean storeTermVectorPositions() {
        return this.storeTermVectorPositions;
    }

    @Override
    public float boost() {
        return this.boost;
    }

    @Override
    public boolean omitNorms() {
        return this.omitNorms;
    }

    @Override
    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override
    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override
    public Analyzer searchQuoteAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            Field field = parseCreateField(context);
            if (field == null) {
                return;
            }
            if (!customBoost()) {
                field.setBoost(boost);
            }
            if (context.listener().beforeFieldAdded(this, field, context)) {
                context.doc().add(field);
            }
        } catch (Exception e) {
            throw new MapperParsingException("Failed to parse [" + names.fullName() + "]", e);
        }
    }

    protected abstract Field parseCreateField(ParseContext context) throws IOException;

    /**
     * Derived classes can override it to specify that boost value is set by derived classes.
     */
    protected boolean customBoost() {
        return false;
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
        // nothing to do here...
    }

    @Override
    public Object valueForSearch(IndexableField field) {
        return valueAsString(field);
    }

    @Override
    public String indexedValue(String value) {
        return value;
    }

    @Override
    public Query queryStringTermQuery(Term term) {
        return null;
    }

    @Override
    public boolean useFieldQueryWithQueryString() {
        return false;
    }

    @Override
    public Query fieldQuery(String value, @Nullable QueryParseContext context) {
        return new TermQuery(names().createIndexNameTerm(indexedValue(value)));
    }

    @Override
    public Filter fieldFilter(String value, @Nullable QueryParseContext context) {
        return new TermFilter(names().createIndexNameTerm(indexedValue(value)));
    }

    @Override
    public Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions) {
        return new FuzzyQuery(new Term(names().indexName(), indexedValue(value)),  prefixLength, maxExpansions);
    }

    @Override
    public Query fuzzyQuery(String value, double minSim, int prefixLength, int maxExpansions) {
        return new FuzzyQuery(new Term(names().indexName(), indexedValue(value)),  prefixLength, maxExpansions);
    }

    @Override
    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        PrefixQuery query = new PrefixQuery(names().createIndexNameTerm(indexedValue(value)));
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Filter prefixFilter(String value, @Nullable QueryParseContext context) {
        return new PrefixFilter(names().createIndexNameTerm(indexedValue(value)));
    }

    @Override
    public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return new TermRangeQuery(names.indexName(),
                lowerTerm == null ? null : new BytesRef(indexedValue(lowerTerm)),
                upperTerm == null ? null : new BytesRef(indexedValue(upperTerm)),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return new TermRangeFilter(names.indexName(),
                lowerTerm == null ? null : new BytesRef(indexedValue(lowerTerm)),
                upperTerm == null ? null : new BytesRef(indexedValue(upperTerm)),
                includeLower, includeUpper);
    }

    @Override
    public Filter nullValueFilter() {
        return null;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        if (!this.getClass().equals(mergeWith.getClass())) {
            String mergedType = mergeWith.getClass().getSimpleName();
            if (mergeWith instanceof AbstractFieldMapper) {
                mergedType = ((AbstractFieldMapper) mergeWith).contentType();
            }
            mergeContext.addConflict("mapper [" + names.fullName() + "] of different type, current_type [" + contentType() + "], merged_type [" + mergedType + "]");
            // different types, return
            return;
        }
        AbstractFieldMapper fieldMergeWith = (AbstractFieldMapper) mergeWith;
        if (this.indexed() != fieldMergeWith.indexed()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index values");
        }
        if (this.tokenized() != fieldMergeWith.tokenized()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different tokenize values");
        }
        if (this.stored() != fieldMergeWith.stored()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store values");
        }
        if (!this.termVector.equals(fieldMergeWith.termVector)) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different term_vector values");
        }
        if (this.storeTermVectorOffsets() != fieldMergeWith.storeTermVectorOffsets()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different term_vector offset values");
        }
        if (this.storeTermVectorPositions() != fieldMergeWith.storeTermVectorPositions()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different term_vector positions values");
        }
        if (this.indexAnalyzer == null) {
            if (fieldMergeWith.indexAnalyzer != null) {
                mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
            }
        } else if (fieldMergeWith.indexAnalyzer == null) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
        } else if (!this.indexAnalyzer.name().equals(fieldMergeWith.indexAnalyzer.name())) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
        }
        if (this.searchAnalyzer == null) {
            if (fieldMergeWith.searchAnalyzer != null) {
                mergeContext.addConflict("mapper [" + names.fullName() + "] has different search_analyzer");
            }
        } else if (fieldMergeWith.searchAnalyzer == null) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different search_analyzer");
        } else if (!this.searchAnalyzer.name().equals(fieldMergeWith.searchAnalyzer.name())) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different search_analyzer");
        }
        if (!mergeContext.mergeFlags().simulate()) {
            // apply changeable values
            this.boost = fieldMergeWith.boost;
        }
    }

    @Override
    public FieldDataType fieldDataType() {
        return FieldDataType.DefaultTypes.STRING;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(names.name());
        doXContentBody(builder);
        builder.endObject();
        return builder;
    }

    protected void doXContentBody(XContentBuilder builder) throws IOException {
        builder.field("type", contentType());
        if (!names.name().equals(names.indexNameClean())) {
            builder.field("index_name", names.indexNameClean());
        }
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        if (indexAnalyzer != null && searchAnalyzer != null && indexAnalyzer.name().equals(searchAnalyzer.name()) && !indexAnalyzer.name().startsWith("_") && !indexAnalyzer.name().equals("default")) {
            // same analyzers, output it once
            builder.field("analyzer", indexAnalyzer.name());
        } else {
            if (indexAnalyzer != null && !indexAnalyzer.name().startsWith("_") && !indexAnalyzer.name().equals("default")) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
            if (searchAnalyzer != null && !searchAnalyzer.name().startsWith("_") && !searchAnalyzer.name().equals("default")) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        }
    }

    protected abstract String contentType();

    @Override
    public void close() {
        // nothing to do here, sub classes to override if needed
    }

}
