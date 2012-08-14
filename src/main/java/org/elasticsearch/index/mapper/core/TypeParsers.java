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

import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;

/**
 *
 */
public class TypeParsers {

    public static void parseNumberField(NumberFieldMapper.Builder builder, String name, Map<String, Object> numberNode, Mapper.TypeParser.ParserContext parserContext) {
        parseField(builder, name, numberNode, parserContext);
        for (Map.Entry<String, Object> entry : numberNode.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
            Object propNode = entry.getValue();
            if (propName.equals("precision_step")) {
                builder.precisionStep(nodeIntegerValue(propNode));
            } else if (propName.equals("fuzzy_factor")) {
                builder.fuzzyFactor(propNode.toString());
            } else if (propName.equals("ignore_malformed")) {
                builder.ignoreMalformed(nodeBooleanValue(propNode));
            }
        }
    }

    public static void parseField(AbstractFieldMapper.Builder builder, String name, Map<String, Object> fieldNode, Mapper.TypeParser.ParserContext parserContext) {
        for (Map.Entry<String, Object> entry : fieldNode.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
            Object propNode = entry.getValue();
            if (propName.equals("index_name")) {
                builder.indexName(propNode.toString());
            } else if (propName.equals("store")) {
                builder.store(parseStore(name, propNode.toString()));
            } else if (propName.equals("index")) {
                String value = propNode.toString();
                if (value.equals("tokenized")) {
                    builder.index(true);
                    builder.tokenize(true);
                } else if (value.equals("analyzed")) {
                    builder.index(true);
                    builder.tokenize(true);
                } else if (value.equals("not_analyzed")) {
                    builder.index(true);
                    builder.tokenize(false);
                } else {
                    builder.index(parseIndex(name, value));
                }

            } else if (propName.equals("tokenize")) {
                builder.tokenize(parseTokenize(name, propNode.toString()));
            } else if (propName.equals("term_vector")) {
                String tvtype = propNode.toString();
                builder.storeTermVectors(parseStoreTermVector(name, tvtype));
                if (tvtype.startsWith("with_")) {
                    builder.storeTermVectorOffsets(parseStoreTermVectorOffsets(name, tvtype));
                    builder.storeTermVectorPositions(parseStoreTermVectorPositions(name, tvtype));
                }
            } else if (propName.equals("term_vector_offsets")) {
                builder.storeTermVectorOffsets(parseStoreTermVectorOffsets(name, propNode.toString()));
            } else if (propName.equals("term_vector_positions")) {
                builder.storeTermVectorPositions(parseStoreTermVectorPositions(name, propNode.toString()));
            } else if (propName.equals("boost")) {
                builder.boost(nodeFloatValue(propNode));
            } else if (propName.equals("omit_norms")) {
                builder.omitNorms(nodeBooleanValue(propNode));
            } else if (propName.equals("omit_term_freq_and_positions")) {
                boolean omit = nodeBooleanValue(propNode);
                if (omit == true) {
                    builder.indexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
                } else {
                    builder.indexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                }
            } else if (propName.equals("analyzer") && propNode != null) {
                if (propNode != null) {
                    NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                    }
                    builder.indexAnalyzer(analyzer);
                    builder.searchAnalyzer(analyzer);
                }
            } else if (propName.equals("index_analyzer") && propNode != null) {
                if (propNode != null) {
                    NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                    }
                    builder.indexAnalyzer(analyzer);
                }
            } else if (propName.equals("search_analyzer") && propNode != null) {
                NamedAnalyzer analyzer = parserContext.analysisService().analyzer(propNode.toString());
                if (analyzer == null) {
                    throw new MapperParsingException("Analyzer [" + propNode.toString() + "] not found for field [" + name + "]");
                }
                builder.searchAnalyzer(analyzer);
            } else if (propName.equals("include_in_all")) {
                builder.includeInAll(nodeBooleanValue(propNode));
            }
        }
    }

    public static FormatDateTimeFormatter parseDateTimeFormatter(String fieldName, Object node) {
        return Joda.forPattern(node.toString());
    }

    public static boolean parseStoreTermVector(String fieldName, String termVector) throws MapperParsingException {
        termVector = Strings.toUnderscoreCase(termVector);
        if ("no".equals(termVector) || "false".equals(termVector)) {
            return false;
        } else if ("yes".equals(termVector) || "true".equals(termVector)) {
            return true;
        } else if ("with_offsets".equals(termVector)) {
            return true;
        } else if ("with_positions".equals(termVector)) {
            return true;
        } else if ("with_positions_offsets".equals(termVector)) {
            return true;
        } else {
            throw new MapperParsingException("Wrong value for termVector [" + termVector + "] for field [" + fieldName + "]");
        }
    }

    public static boolean parseStoreTermVectorOffsets(String fieldName, String termVector) throws MapperParsingException {
        termVector = Strings.toUnderscoreCase(termVector);
        if ("no".equals(termVector)) {
            return false;
        } else if ("yes".equals(termVector) || "true".equals(termVector)) {
            return true;
        } else if ("with_offsets".equals(termVector)) {
            return true;
        } else if ("with_positions_offsets".equals(termVector)) {
            return true;
        }
        return false;
    }

    public static boolean parseStoreTermVectorPositions(String fieldName, String termVector) throws MapperParsingException {
        termVector = Strings.toUnderscoreCase(termVector);
        if ("no".equals(termVector)) {
            return false;
        } else if ("yes".equals(termVector) || "true".equals(termVector)) {
            return true;
        } else if ("with_positions".equals(termVector)) {
            return true;
        } else if ("with_positions_offsets".equals(termVector)) {
            return true;
        }
        return false;
    }

    public static boolean parseIndex(String fieldName, String index) throws MapperParsingException {
        index = Strings.toUnderscoreCase(index);
        if ("no".equals(index) || "false".equals(index)) {
            return false;
        } else if ("yes".equals(index) || "true".equals(index) || "not_analyzed".equals(index)) {
            return true;
        } else {
            throw new MapperParsingException("Wrong value for index [" + index + "] for field [" + fieldName + "]");
        }
    }

    public static boolean parseTokenize(String fieldName, String tokenize) throws MapperParsingException {
        tokenize = Strings.toUnderscoreCase(tokenize);
        if ("no".equals(tokenize) || "false".equals(tokenize)) {
            return false;
        } else if ("yes".equals(tokenize) || "true".equals(tokenize)) {
            return true;
        } else {
            throw new MapperParsingException("Wrong value for index [" + tokenize + "] for field [" + fieldName + "]");
        }
    }


    public static boolean parseStore(String fieldName, String store) throws MapperParsingException {
        if ("no".equals(store)) {
            return false;
        } else if ("yes".equals(store)) {
            return true;
        } else {
            boolean value = nodeBooleanValue(store);
            if (value) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static ContentPath.Type parsePathType(String name, String path) throws MapperParsingException {
        path = Strings.toUnderscoreCase(path);
        if ("just_name".equals(path)) {
            return ContentPath.Type.JUST_NAME;
        } else if ("full".equals(path)) {
            return ContentPath.Type.FULL;
        } else {
            throw new MapperParsingException("Wrong value for pathType [" + path + "] for object [" + name + "]");
        }
    }

}
