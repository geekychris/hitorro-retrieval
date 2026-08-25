/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.stream;

/**
 * A single source's contribution to a cross-index merged search.
 * Frame iterator producing:
 *
 * <pre>
 *   DocFrame … DocFrame  →  AggregateFrame … AggregateFrame  →  Eos
 * </pre>
 *
 * <p>Aggregates always follow the documents (matches the pipeline's
 * natural order: documents flow through IndexRetriever + PaginationRetriever
 * first, then aggregates are materialized from RetrievalContext at
 * end-of-pipeline). The k-way merger relies on this ordering to
 * detect end-of-docs on a per-source basis without consulting Eos.</p>
 *
 * <p>{@link AutoCloseable} because sources may hold reader / iterator
 * resources; merger closes every stream in a finally block regardless
 * of whether it drained them fully (early-limit is normal).</p>
 */
public interface SearchStream extends AutoCloseable {
    /** Human-readable source name — usually the Lucene index name.
     *  Used as the key when the merger builds byIndex sub-objects. */
    String sourceName();

    /** True if {@link #next()} would return a non-null frame. Sticky
     *  false after {@link StreamFrame.Eos} has been consumed. */
    boolean hasNext();

    /** Next frame from this stream. Behaviour undefined after
     *  {@link StreamFrame.Eos} has been returned once — impls may
     *  return Eos repeatedly or throw. Callers should stop pulling
     *  after Eos. */
    StreamFrame next();

    @Override
    default void close() { /* no resources for the common in-memory case */ }
}
