/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.stream;

import com.hitorro.index.search.SearchResult;
import com.hitorro.retrieval.aggregate.FacetAggregate;
import com.hitorro.retrieval.aggregate.RetrievalAggregate;
import com.hitorro.retrieval.aggregate.SearchSummaryAggregate;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Wraps a materialized {@link SearchResult} + optional trailing
 * aggregates into a {@link SearchStream}. Used by the coordinator
 * to bridge per-provider search results (which come back as one
 * complete SearchResult) into the streaming k-way merger.
 *
 * <p>Emission order: every document → every aggregate → Eos.
 * Aggregates come from two sources: the SearchResult-derived
 * {@link SearchSummaryAggregate} + {@link FacetAggregate} (built
 * automatically with the SearchResult captured for merge-time), plus
 * any extra aggregates the caller passes (e.g. SummarizationAggregate
 * from the summarize stage).</p>
 *
 * <p>Every aggregate emitted here carries the SearchResult in its
 * capture slot when applicable — required by the merger's aggregate-
 * merge policies (see the aggregate types' mergeWith impls).</p>
 */
public final class IteratorSearchStream implements SearchStream {

    private final String sourceName;
    private final Deque<StreamFrame> queue;

    public IteratorSearchStream(String sourceName, SearchResult sr,
                                List<RetrievalAggregate> extraAggregates) {
        this.sourceName = sourceName;
        this.queue = new ArrayDeque<>();

        // Docs first.
        if (sr != null && sr.getDocuments() != null) {
            for (var doc : sr.getDocuments()) queue.add(new StreamFrame.DocFrame(doc));
        }

        // Built-in aggregates derived from the SearchResult. Each
        // captures the SR so mergeWith at the merger side has real
        // data to combine — the merger builds a synthetic context
        // that doesn't otherwise carry the source's SearchResult.
        if (sr != null) {
            queue.add(new StreamFrame.AggregateFrame(new SearchSummaryAggregate(sr)));
            if (sr.hasFacets()) {
                queue.add(new StreamFrame.AggregateFrame(new FacetAggregate(sr)));
            }
        }

        // Extra aggregates from the pipeline (e.g. SummarizationAggregate).
        // Order preserved.
        if (extraAggregates != null) {
            for (var agg : extraAggregates) queue.add(new StreamFrame.AggregateFrame(agg));
        }

        // Terminal marker so hasNext returns false after Eos consumed.
        queue.add(StreamFrame.Eos.INSTANCE);
    }

    @Override public String sourceName() { return sourceName; }
    @Override public boolean hasNext()  { return !queue.isEmpty(); }
    @Override public StreamFrame next() { return queue.poll(); }
}
