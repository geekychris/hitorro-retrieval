/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.aggregate;

import com.hitorro.index.search.FacetResult;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.merger.ResultMerger;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Produces a facet aggregate from the Lucene SearchResult's facet data.
 *
 * <p>Two constructors mirror {@link SearchSummaryAggregate} — no-arg
 * legacy (reads from context) and a {@link SearchResult}-capturing
 * merger-friendly variant.</p>
 *
 * <p>Merge policies:</p>
 * <ul>
 *   <li>{@link MergePolicy#SUMMED} (default) — reuses
 *       {@link ResultMerger#mergeFacets} to union per-value counts.
 *       Adds a {@code byIndex.<sourceName>} sub-object carrying each
 *       source's raw facets.</li>
 *   <li>{@link MergePolicy#PER_INDEX} — only {@code byIndex}; top-level
 *       dimensions omitted.</li>
 *   <li>Unsupported → falls back to SUMMED.</li>
 * </ul>
 */
public class FacetAggregate implements RetrievalAggregate {

    /** Captured per-source SearchResult — merger reads siblings' captures.
     *  Package-private so mergeWith can access without a getter zoo. */
    final SearchResult captured;

    public FacetAggregate() { this.captured = null; }
    public FacetAggregate(SearchResult captured) { this.captured = captured; }

    @Override
    public String aggregateName() { return "facets"; }

    @Override
    public MergePolicy defaultPolicy() { return MergePolicy.SUMMED; }

    @Override
    public Set<MergePolicy> supportedPolicies() {
        return EnumSet.of(MergePolicy.SUMMED, MergePolicy.PER_INDEX);
    }

    @Override
    public JVS toJVS(RetrievalContext context) {
        SearchResult sr = captured != null ? captured : context.getSearchResult();
        if (sr == null || !sr.hasFacets()) return null;

        JVS facets = sr.toFacetsJVS();
        if (facets != null) facets.set("_aggregate", "facets");
        return facets;
    }

    @Override
    public RetrievalAggregate mergeWith(
            List<? extends RetrievalAggregate> others,
            MergePolicy policy,
            Map<String, ? extends RetrievalAggregate> byIndex,
            RetrievalContext context) {

        if (!supportedPolicies().contains(policy)) policy = defaultPolicy();

        // Per-source SearchResults + per-source facet JVSes. Skip sources
        // that had no facets — mergeFacets handles the empties.
        List<SearchResult> sources = new ArrayList<>();
        JVS byIndexJson = new JVS();
        for (Map.Entry<String, ? extends RetrievalAggregate> e : byIndex.entrySet()) {
            RetrievalAggregate a = e.getValue();
            SearchResult sr = (a instanceof FacetAggregate f) ? f.captured : null;
            if (sr == null || !sr.hasFacets()) continue;
            sources.add(sr);
            JVS perSource = sr.toFacetsJVS();
            // JVS.set on Object arg doesn't recognise nested JVS —
            // unwrap to JsonNode so it lands as a sub-object.
            if (perSource != null) byIndexJson.set(e.getKey(), perSource.getJsonNode());
        }

        JVS merged = new JVS();
        merged.set("_aggregate", "facets");

        if (policy == MergePolicy.SUMMED) {
            // Reuse the existing static helper — unions (dim, value) counts
            // across sources, caps at 100 per dim, sorts by count desc.
            Map<String, FacetResult> unionMap = ResultMerger.mergeFacets(sources);
            if (unionMap != null) {
                for (Map.Entry<String, FacetResult> fe : unionMap.entrySet()) {
                    // toJVS() returns Jackson ObjectNode already; passes
                    // through ensureJsonNode cleanly.
                    merged.set(fe.getKey(), fe.getValue().toJVS());
                }
            }
        }
        // PER_INDEX: only byIndex sub-object.

        merged.set("byIndex", byIndexJson.getJsonNode());
        return new SnapshotAggregate(aggregateName(), merged);
    }
}
