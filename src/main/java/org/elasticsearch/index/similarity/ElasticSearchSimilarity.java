package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.index.IndexComponent;

/**
 * @author aparo (alberto.paro)
 */
public interface ElasticSearchSimilarity<T extends Similarity> extends IndexComponent, Provider<T> {

    String name();

    T get();
}
