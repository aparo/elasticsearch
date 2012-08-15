package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 * @author aparo (alberto.paro)
 */
public class DefaultSimilarityProviderFactory extends AbstractSimilarityProvider<Similarity> {

    private Similarity similarity;

    @Inject
    public DefaultSimilarityProviderFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
        this.similarity = new DefaultSimilarity();
    }

    public Similarity get() {
        return similarity;
    }

}
