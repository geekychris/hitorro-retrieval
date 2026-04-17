package com.hitorro.retrieval.aggregate;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

/**
 * A side-channel result produced by a pipeline stage.
 *
 * <p>Aggregates are collected alongside the main document stream and
 * materialized after iteration completes. Examples: search summary
 * (totalHits, timing), facet counts, cluster results.
 */
public interface RetrievalAggregate {

    /**
     * Materializes this aggregate as a JVS document.
     *
     * @param context the retrieval context (provides access to SearchResult, etc.)
     * @return a JVS document representing this aggregate, or null if nothing to report
     */
    JVS toJVS(RetrievalContext context);
}
