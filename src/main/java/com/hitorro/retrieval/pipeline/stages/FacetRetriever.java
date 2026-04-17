package com.hitorro.retrieval.pipeline.stages;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.aggregate.FacetAggregate;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;

/**
 * Registers the facet aggregate if facets are available in the SearchResult.
 *
 * <p>Does not modify the document stream -- it only registers a
 * {@link FacetAggregate} so facet data appears in the retrieval output.
 * The actual facet computation happened during {@link IndexRetriever}'s
 * Lucene search; this stage just exposes those results.
 */
public class FacetRetriever implements Retriever {

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        return context.getSearchResult() != null && context.getSearchResult().hasFacets();
    }

    @Override
    public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
        context.addAggregate(new FacetAggregate());
        return input;
    }
}
