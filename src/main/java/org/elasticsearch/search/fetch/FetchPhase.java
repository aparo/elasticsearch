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

package org.elasticsearch.search.fetch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.matchedfilters.MatchedFiltersFetchSubPhase;
import org.elasticsearch.search.fetch.partial.PartialFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class FetchPhase implements SearchPhase {

    private final FetchSubPhase[] fetchSubPhases;

    @Inject
    public FetchPhase(HighlightPhase highlightPhase, ScriptFieldsFetchSubPhase scriptFieldsPhase, PartialFieldsFetchSubPhase partialFieldsPhase,
                      MatchedFiltersFetchSubPhase matchFiltersPhase, ExplainFetchSubPhase explainPhase, VersionFetchSubPhase versionPhase) {
        this.fetchSubPhases = new FetchSubPhase[]{scriptFieldsPhase, partialFieldsPhase, matchFiltersPhase, explainPhase, highlightPhase, versionPhase};
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("fields", new FieldsParseElement());
        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            parseElements.putAll(fetchSubPhase.parseElements());
        }
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    public void execute(SearchContext context) {
        List<String> extractFieldNames = null;
        Set<String> fieldSelector = new HashSet<String>();
        boolean sourceRequested = false;
        if (!context.hasFieldNames()) {
            if (context.hasPartialFields()) {
                // partial fields need the source, so fetch it, but don't return it
                fieldSelector.add(UidFieldMapper.NAME);
                fieldSelector.add(SourceFieldMapper.NAME);
                fieldSelector.add(RoutingFieldMapper.NAME);
                sourceRequested = false;
            } else if (context.hasScriptFields()) {
                // we ask for script fields, and no field names, don't load the source
                fieldSelector.add(UidFieldMapper.NAME);
                fieldSelector.add(RoutingFieldMapper.NAME);
                sourceRequested = false;
            } else {
                fieldSelector.add(UidFieldMapper.NAME);
                fieldSelector.add(SourceFieldMapper.NAME);
                fieldSelector.add(RoutingFieldMapper.NAME);
                sourceRequested = true;
            }

        } else if (context.fieldNames().isEmpty()) {
            fieldSelector.add(UidFieldMapper.NAME);
            sourceRequested = false;
        } else {
            boolean loadAllStored = false;
            for (String fieldName : context.fieldNames()) {
                if (fieldName.equals("*")) {
                    loadAllStored = true;
                    continue;
                }
                if (fieldName.equals(SourceFieldMapper.NAME)) {
                    sourceRequested = true;
                    continue;
                }
                FieldMappers x = context.smartNameFieldMappers(fieldName);
                if (x != null && x.mapper().stored()) {
                    fieldSelector.add(fieldName);
                } else {
                    if (extractFieldNames == null) {
                        extractFieldNames = Lists.newArrayList();
                    }
                    extractFieldNames.add(fieldName);
                }
            }

            if (loadAllStored) {
                if (sourceRequested || extractFieldNames != null) {
                    fieldSelector = null; // load everything, including _source     //PARO TO CHECK
                } else {
                    fieldSelector.clear();
                }
            } else if (fieldSelector.size() > 0) {
                // we are asking specific stored fields, just add the UID and be done
                fieldSelector.add(UidFieldMapper.NAME);
                fieldSelector.add(RoutingFieldMapper.NAME);
                if (extractFieldNames != null) {
                    fieldSelector.add(SourceFieldMapper.NAME);
                }
            } else if (extractFieldNames != null || sourceRequested) {
                fieldSelector.add(UidFieldMapper.NAME);
                fieldSelector.add(SourceFieldMapper.NAME);
                fieldSelector.add(RoutingFieldMapper.NAME);
            } else {
                fieldSelector.add(UidFieldMapper.NAME);
                fieldSelector.add(RoutingFieldMapper.NAME);
            }
        }

        InternalSearchHit[] hits = new InternalSearchHit[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];
            Document doc = loadDocument(context, fieldSelector, docId);
            Uid uid = extractUid(context, doc);
            String routing = extractRouting(context, doc);

            DocumentMapper documentMapper = context.mapperService().documentMapper(uid.type());

            if (documentMapper == null) {
                throw new TypeMissingException(new Index(context.shardTarget().index()), uid.type(), "failed to find type loaded for doc [" + uid.id() + "]");
            }

            byte[] source = extractSource(doc, documentMapper);

            // get the version

            InternalSearchHit searchHit = new InternalSearchHit(docId, uid.id(), uid.type(), sourceRequested ? source : null, null);
            hits[index] = searchHit;

            for (Object oField : doc.getFields()) {
                IndexableField field = (IndexableField) oField;
                String name = field.name();

                // ignore UID, we handled it above
                if (name.equals(UidFieldMapper.NAME)) {
                    continue;
                }

                // ignore source, we handled it above
                if (name.equals(SourceFieldMapper.NAME)) {
                    continue;
                }

                // ignore routing, we handled it above
                if (name.equals(RoutingFieldMapper.NAME)) {
                    continue;
                }

                Object value = null;
                FieldMappers fieldMappers = documentMapper.mappers().indexName(field.name());
                if (fieldMappers != null) {
                    FieldMapper mapper = fieldMappers.mapper();
                    if (mapper != null) {
                        name = mapper.names().fullName();
                        value = mapper.valueForSearch(field);
                    }
                }
                if (value == null) {
                    value = new BytesArray(field.binaryValue().bytes);
                }
                if (value == null)
                    value = field.stringValue();

                if (searchHit.fieldsOrNull() == null) {
                    searchHit.fields(new HashMap<String, SearchHitField>(2));
                }

                SearchHitField hitField = searchHit.fields().get(name);
                if (hitField == null) {
                    hitField = new InternalSearchHitField(name, new ArrayList<Object>(2));
                    searchHit.fields().put(name, hitField);
                }
                hitField.values().add(value);
            }

            //int readerIndex = context.searcher().readerIndex(docId);
            //IndexReader subReader = context.searcher().subReaders()[readerIndex];
            //int subDoc = docId - context.searcher().docStarts()[readerIndex];
            AtomicReaderContext readerContextIndex = context.searcher().contextIndex(docId);
            //IndexReader subReader = readerContextIndex.reader;
            int subDoc = docId - readerContextIndex.docBase;
            //TODO: PARO: check subdoc


            // go over and extract fields that are not mapped / stored
            context.lookup().setNextReader(readerContextIndex);
            context.lookup().setNextDocId(subDoc);
            if (source != null) {
                context.lookup().source().setNextSource(new BytesArray(source));
            }
            if (extractFieldNames != null) {
                for (String extractFieldName : extractFieldNames) {
                    if (extractFieldName.equals("_parent")) {
                        SearchHitField hitField = searchHit.fields().get(extractFieldName);
                        if (hitField == null) {
                            hitField = new InternalSearchHitField(extractFieldName, new ArrayList<Object>(2));
                            searchHit.fields().put(extractFieldName, hitField);
                        }
                        hitField.values().add(routing);
                    }

                    Object value = context.lookup().source().extractValue(extractFieldName);
                    if (value != null) {
                        if (searchHit.fieldsOrNull() == null) {
                            searchHit.fields(new HashMap<String, SearchHitField>(2));
                        }

                        SearchHitField hitField = searchHit.fields().get(extractFieldName);
                        if (hitField == null) {
                            hitField = new InternalSearchHitField(extractFieldName, new ArrayList<Object>(2));
                            searchHit.fields().put(extractFieldName, hitField);
                        }
                        hitField.values().add(value);
                    }
                }
            }

            for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
                if (fetchSubPhase.hitExecutionNeeded(context)) {
                    hitContext.reset(searchHit, readerContextIndex.reader(), subDoc, context.searcher().getIndexReader(), docId, doc);
                    fetchSubPhase.hitExecute(context, hitContext);
                }
            }

        }

        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            if (fetchSubPhase.hitsExecutionNeeded(context)) {
                fetchSubPhase.hitsExecute(context, hits);
            }
        }

        context.fetchResult().hits(new InternalSearchHits(hits, context.queryResult().topDocs().totalHits, context.queryResult().topDocs().getMaxScore()));
    }

    private byte[] extractSource(Document doc, DocumentMapper documentMapper) {
        IndexableField sourceField = doc.getField(SourceFieldMapper.NAME);
        if (sourceField != null) {
            return documentMapper.sourceMapper().nativeValue(sourceField);
        }
        return null;
    }

    private String extractRouting(SearchContext context, Document doc) {
        String routing = doc.get(RoutingFieldMapper.NAME);
        if (routing != null) {
            return routing;
        }
        return doc.get(ParentFieldMapper.NAME);
    }

    private Uid extractUid(SearchContext context, Document doc) {
        // TODO we might want to use FieldData here to speed things up, so we don't have to load it at all...
        String sUid = doc.get(UidFieldMapper.NAME);
        if (sUid != null) {
            return Uid.createUid(sUid);
        }
        // no type, nothing to do (should not really happen)
        List<String> fieldNames = new ArrayList<String>();
        for (IndexableField field : doc.getFields()) {
            fieldNames.add(field.name());
        }
        throw new FetchPhaseExecutionException(context, "Failed to load uid from the index, missing internal _uid field, current fields in the doc [" + fieldNames + "]");
    }

    private Document loadDocument(SearchContext context, Set<String> fields, int docId) {
        try {
            DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
            if (fields != null && fields.size() > 0) {
                visitor = new DocumentStoredFieldVisitor(fields);
            }
            context.searcher().getIndexReader().document(docId, visitor);
            return visitor.getDocument();
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch doc id [" + docId + "]", e);
        }
    }
}
