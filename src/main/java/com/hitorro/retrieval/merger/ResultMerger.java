package com.hitorro.retrieval.merger;

import com.hitorro.index.search.FacetResult;
import com.hitorro.index.search.SearchResult;

import java.util.*;

/**
 * Merges multiple {@link SearchResult} instances from different providers
 * into a single unified result, respecting sort order.
 */
public interface ResultMerger {

    SearchResult merge(List<SearchResult> results, int offset, int limit);

    SearchResult merge(List<SearchResult> results, int offset, int limit, SortCriteria criteria);

    String getName();

    /**
     * Merge facets from multiple SearchResults by summing counts per dimension/value.
     * Returns null if no source has facets.
     */
    static Map<String, FacetResult> mergeFacets(List<SearchResult> results) {
        Map<String, Map<String, Long>> merged = new LinkedHashMap<>();

        boolean anyFacets = false;
        for (SearchResult sr : results) {
            if (!sr.hasFacets()) continue;
            anyFacets = true;
            for (var entry : sr.getFacets().entrySet()) {
                String dim = entry.getKey();
                Map<String, Long> dimCounts = merged.computeIfAbsent(dim, k -> new LinkedHashMap<>());
                for (FacetResult.FacetValue fv : entry.getValue().getValues()) {
                    dimCounts.merge(fv.getValue(), fv.getCount(), Long::sum);
                }
            }
        }

        if (!anyFacets) return null;

        Map<String, FacetResult> result = new LinkedHashMap<>();
        for (var entry : merged.entrySet()) {
            String dim = entry.getKey();
            Map<String, Long> counts = entry.getValue();
            long total = counts.values().stream().mapToLong(Long::longValue).sum();
            List<FacetResult.FacetValue> values = counts.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                    .limit(100)
                    .map(e -> new FacetResult.FacetValue(e.getKey(), e.getValue()))
                    .toList();
            result.put(dim, new FacetResult(dim, values, total));
        }
        return result;
    }
}
