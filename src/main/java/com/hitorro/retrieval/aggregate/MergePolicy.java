/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.aggregate;

/**
 * How multiple copies of the same aggregate type (one per federated
 * source) should be combined into the single aggregate the caller sees.
 *
 * <p>Each concrete {@link RetrievalAggregate} implementation declares
 * which policies it supports via {@link RetrievalAggregate#supportedPolicies()}.
 * When the coordinator hands the merger a policy the aggregate doesn't
 * support, the aggregate silently falls back to its
 * {@link RetrievalAggregate#defaultPolicy()} — merger stays a no-op for
 * unknown aggregate types.</p>
 *
 * <p>Every policy MUST emit a {@code byIndex.&lt;sourceName&gt;} sub-object
 * on the merged result carrying the per-source view, so a consumer can
 * always drill in even when the top-level fields reflect e.g. a SUMMED
 * total. Single-source runs (source-count == 1) skip the {@code byIndex}
 * emission — merged output stays byte-identical to non-federated today.</p>
 */
public enum MergePolicy {
    /**
     * Numeric fields sum (totalHits across sources), timing takes the
     * max (wall time is the slowest branch), value-count facets union
     * per (dim, value). The bread-and-butter policy for
     * {@link SearchSummaryAggregate} and {@link FacetAggregate}.
     */
    SUMMED,

    /**
     * Emit the merged view as {@code byIndex.&lt;sourceName&gt;} only —
     * top-level scalar fields either come from the first source or
     * are omitted, depending on the aggregate. Useful when the caller
     * always wants to see per-source detail and doesn't want a
     * confusing "combined" number.
     */
    PER_INDEX,

    /**
     * Join per-source payloads with source-labelled delimiters into a
     * single string / body. Meaningful for text-shaped aggregates
     * like {@link SummarizationAggregate}; nonsense for numeric
     * summaries — impls should exclude it from
     * {@link RetrievalAggregate#supportedPolicies()} when it doesn't apply.
     */
    CONCATENATED,

    /**
     * Concatenate the per-source payloads and hand the combined blob
     * back to an AI summariser to produce ONE overall summary. Uses
     * additional model tokens per federated search — expensive; only
     * supported by aggregates that hold text and have access to the
     * summariser (typically {@link SummarizationAggregate}).
     */
    RESUMMARIZED,

    /**
     * Take the first source's aggregate verbatim. A cheap fallback for
     * types that have no natural merge — the {@code byIndex} sub-object
     * still carries all sources so consumers aren't blind.
     */
    PICK_FIRST
}
