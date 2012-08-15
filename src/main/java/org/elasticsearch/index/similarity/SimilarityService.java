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

package org.elasticsearch.index.similarity;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class SimilarityService extends AbstractIndexComponent {

    private final ImmutableMap<String, ElasticSearchSimilarity> similarityProviders;

    public SimilarityService(Index index) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, null);
    }

    @Inject
    public SimilarityService(Index index, @IndexSettings Settings indexSettings,
                             @Nullable Map<String, SimilarityProviderFactory> providerFactories) {
        super(index, indexSettings);

        Map<String, ElasticSearchSimilarity> similarityProviders = newHashMap();
        if (providerFactories != null) {
            Map<String, Settings> providersSettings = indexSettings.getGroups("index.similarity");
            for (Map.Entry<String, SimilarityProviderFactory> entry : providerFactories.entrySet()) {
                String similarityName = entry.getKey();
                SimilarityProviderFactory similarityProviderFactory = entry.getValue();

                Settings similaritySettings = providersSettings.get(similarityName);
                if (similaritySettings == null) {
                    similaritySettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
                }

                ElasticSearchSimilarity similarityProvider = similarityProviderFactory.create(similarityName, similaritySettings);
                similarityProviders.put(similarityName, similarityProvider);
            }
        }

        // add defaults
        if (!similarityProviders.containsKey("index")) {
            similarityProviders.put("index", new DefaultSimilarityProviderFactory(index, indexSettings, "index", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        if (!similarityProviders.containsKey("search")) {
            similarityProviders.put("search", new DefaultSimilarityProviderFactory(index, indexSettings, "search", ImmutableSettings.Builder.EMPTY_SETTINGS));
        }
        this.similarityProviders = ImmutableMap.copyOf(similarityProviders);
    }

    public ElasticSearchSimilarity similarityProvider(String name) {
        return similarityProviders.get(name);
    }

    public ElasticSearchSimilarity defaultIndexSimilarity() {
        return similarityProviders.get("index");

    }

    public ElasticSearchSimilarity defaultSearchSimilarity() {
        return similarityProviders.get("search");

    }
}
