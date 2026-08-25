/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.aggregate;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Produces a summary aggregate from the Lucene SearchResult:
 * totalHits, query, offset, limit, searchTimeMs, returned count.
 *
 * <p>Two constructors:</p>
 * <ul>
 *   <li>{@link #SearchSummaryAggregate()} — legacy: reads SearchResult
 *       from {@link RetrievalContext#getSearchResult()} at toJVS time.
 *       All existing pipeline stages use this shape.</li>
 *   <li>{@link #SearchSummaryAggregate(SearchResult)} — merger-friendly:
 *       carries its own SearchResult snapshot. Needed by the merger
 *       which builds a synthetic RetrievalContext without a full
 *       SearchResult.</li>
 * </ul>
 *
 * <p>Merge policies:</p>
 * <ul>
 *   <li>{@link MergePolicy#SUMMED} (default) — {@code totalHits} sums,
 *       {@code searchTimeMs} takes the max (wall time), {@code query}
 *       comes from the first source (all sources ran the same query
 *       under this coordinator). Adds a {@code byIndex} sub-object.</li>
 *   <li>{@link MergePolicy#PER_INDEX} — {@code byIndex} is the merged
 *       result; no top-level totals.</li>
 *   <li>Unsupported policies fall back to SUMMED.</li>
 * </ul>
 */
public class SearchSummaryAggregate implements RetrievalAggregate {

    /**
     * Optional captured SearchResult. When present, {@link #toJVS}
     * uses it and ignores the context. Package-private so
     * {@link #mergeWith} can read siblings' captures.
     */
    final SearchResult captured;

    public SearchSummaryAggregate() { this.captured = null; }
    public SearchSummaryAggregate(SearchResult captured) { this.captured = captured; }

    @Override
    public String aggregateName() { return "summary"; }

    @Override
    public MergePolicy defaultPolicy() { return MergePolicy.SUMMED; }

    @Override
    public Set<MergePolicy> supportedPolicies() {
        return EnumSet.of(MergePolicy.SUMMED, MergePolicy.PER_INDEX);
    }

    @Override
    public JVS toJVS(RetrievalContext context) {
        SearchResult sr = captured != null ? captured : context.getSearchResult();
        if (sr == null) return null;

        JVS meta = sr.toMetadataJVS();
        if (meta != null) {
            meta.set("_aggregate", "summary");
        }
        return meta;
    }

    @Override
    public RetrievalAggregate mergeWith(
            List<? extends RetrievalAggregate> others,
            MergePolicy policy,
            Map<String, ? extends RetrievalAggregate> byIndex,
            RetrievalContext context) {

        // Fall back to default if the caller requested something we don't do.
        if (!supportedPolicies().contains(policy)) policy = defaultPolicy();

        JVS merged = new JVS();
        merged.set("_aggregate", "summary");

        // Per-source view up front — used by both policies (either as the
        // top-level result or as the byIndex sub-object).
        JVS byIndexJson = new JVS();
        long totalHitsSum = 0;
        long timeMax = 0;
        String queryOne = null;
        int offsetOne = 0, limitOne = 0, returnedSum = 0;
        for (Map.Entry<String, ? extends RetrievalAggregate> e : byIndex.entrySet()) {
            RetrievalAggregate a = e.getValue();
            SearchResult sr = (a instanceof SearchSummaryAggregate s) ? s.captured : null;
            if (sr == null) continue;
            totalHitsSum += sr.getTotalHits();
            timeMax = Math.max(timeMax, sr.getSearchTimeMs());
            if (queryOne == null) { queryOne = sr.getQuery(); offsetOne = sr.getOffset(); limitOne = sr.getLimit(); }
            returnedSum += sr.getDocuments() == null ? 0 : sr.getDocuments().size();

            JVS perSource = sr.toMetadataJVS();
            // JVS.set on an Object arg doesn't recognise nested JVS
            // — unwrap to JsonNode so it actually lands as a sub-object.
            if (perSource != null) byIndexJson.set(e.getKey(), perSource.getJsonNode());
        }

        if (policy == MergePolicy.SUMMED) {
            merged.set("totalHits",    totalHitsSum);
            merged.set("query",        queryOne);
            merged.set("offset",       offsetOne);
            merged.set("limit",        limitOne);
            merged.set("searchTimeMs", timeMax);
            merged.set("returned",     returnedSum);
        }
        // PER_INDEX: no top-level totals, only byIndex.

        merged.set("byIndex", byIndexJson.getJsonNode());
        return new SnapshotAggregate(aggregateName(), merged);
    }
}
