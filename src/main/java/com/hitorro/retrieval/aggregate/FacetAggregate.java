package com.hitorro.retrieval.aggregate;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

/**
 * Produces a facet aggregate from the Lucene SearchResult's facet data.
 */
public class FacetAggregate implements RetrievalAggregate {

    @Override
    public JVS toJVS(RetrievalContext context) {
        SearchResult sr = context.getSearchResult();
        if (sr == null || !sr.hasFacets()) return null;

        JVS facets = sr.toFacetsJVS();
        if (facets != null) {
            facets.set("_aggregate", "facets");
        }
        return facets;
    }
}
