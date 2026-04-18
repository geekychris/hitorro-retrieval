package com.hitorro.retrieval.merger;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;

import java.util.*;

/**
 * Merges results by relevance score (descending).
 * Duplicate documents (same _uid) are resolved by keeping the highest score.
 */
public class ScoreMerger implements ResultMerger {

    @Override
    public SearchResult merge(List<SearchResult> results, int offset, int limit) {
        return merge(results, offset, limit, SortCriteria.byScore());
    }

    @Override
    public SearchResult merge(List<SearchResult> results, int offset, int limit, SortCriteria criteria) {
        long totalHits = 0;
        long totalTime = 0;
        Map<String, JVS> deduped = new LinkedHashMap<>();

        for (SearchResult sr : results) {
            totalHits += sr.getTotalHits();
            totalTime = Math.max(totalTime, sr.getSearchTimeMs());
            for (JVS doc : sr.getDocuments()) {
                String uid = extractUid(doc);
                if (uid != null) {
                    JVS existing = deduped.get(uid);
                    if (existing == null || getScore(doc) > getScore(existing)) {
                        deduped.put(uid, doc);
                    }
                } else {
                    deduped.put(UUID.randomUUID().toString(), doc);
                }
            }
        }

        List<JVS> sorted = new ArrayList<>(deduped.values());
        sorted.sort((a, b) -> Double.compare(getScore(b), getScore(a)));

        int start = Math.min(offset, sorted.size());
        int end = Math.min(offset + limit, sorted.size());
        List<JVS> page = sorted.subList(start, end);

        var builder = SearchResult.builder()
                .documents(new ArrayList<>(page))
                .totalHits(totalHits)
                .offset(offset)
                .limit(limit)
                .searchTimeMs(totalTime);
        var mergedFacets = ResultMerger.mergeFacets(results);
        if (mergedFacets != null) builder.facets(mergedFacets);
        return builder.build();
    }

    @Override
    public String getName() { return "score"; }

    private double getScore(JVS doc) {
        try {
            if (doc.exists("_score")) return doc.getDouble("_score");
        } catch (Exception ignored) {}
        return 0;
    }

    private String extractUid(JVS doc) {
        try {
            if (doc.exists("_uid")) return doc.getString("_uid");
            if (doc.exists("id.id")) return doc.getString("id.id");
        } catch (Exception ignored) {}
        return null;
    }
}
