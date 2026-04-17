package com.hitorro.retrieval.aggregate;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

/**
 * Produces a summary aggregate from the Lucene SearchResult:
 * totalHits, query, offset, limit, searchTimeMs, returned count.
 */
public class SearchSummaryAggregate implements RetrievalAggregate {

    @Override
    public JVS toJVS(RetrievalContext context) {
        SearchResult sr = context.getSearchResult();
        if (sr == null) return null;

        JVS meta = sr.toMetadataJVS();
        if (meta != null) {
            meta.set("_aggregate", "summary");
        }
        return meta;
    }
}
