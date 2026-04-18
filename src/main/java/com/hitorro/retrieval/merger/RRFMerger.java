package com.hitorro.retrieval.merger;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;

import java.util.*;

/**
 * Reciprocal Rank Fusion merger. Combines ranked lists from multiple providers
 * using the formula: RRF_score = sum over rankings of 1/(k + rank_i).
 */
public class RRFMerger implements ResultMerger {

    private final int k;

    public RRFMerger() { this(60); }

    public RRFMerger(int k) { this.k = k; }

    @Override
    public SearchResult merge(List<SearchResult> results, int offset, int limit) {
        return merge(results, offset, limit, SortCriteria.byScore());
    }

    @Override
    public SearchResult merge(List<SearchResult> results, int offset, int limit, SortCriteria criteria) {
        long totalHits = 0;
        long totalTime = 0;
        Map<String, Double> rrfScores = new LinkedHashMap<>();
        Map<String, JVS> docMap = new LinkedHashMap<>();

        for (SearchResult sr : results) {
            totalHits += sr.getTotalHits();
            totalTime = Math.max(totalTime, sr.getSearchTimeMs());

            List<JVS> docs = sr.getDocuments();
            for (int rank = 0; rank < docs.size(); rank++) {
                JVS doc = docs.get(rank);
                String uid = extractUid(doc);
                if (uid == null) uid = UUID.randomUUID().toString();

                double rrfContribution = 1.0 / (k + rank + 1);
                rrfScores.merge(uid, rrfContribution, Double::sum);
                docMap.putIfAbsent(uid, doc);
            }
        }

        // Sort by RRF score descending
        List<Map.Entry<String, Double>> sorted = new ArrayList<>(rrfScores.entrySet());
        sorted.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        int start = Math.min(offset, sorted.size());
        int end = Math.min(offset + limit, sorted.size());
        List<JVS> page = new ArrayList<>();
        for (int i = start; i < end; i++) {
            JVS doc = docMap.get(sorted.get(i).getKey());
            if (doc != null) {
                doc.set("_rrf_score", sorted.get(i).getValue());
                page.add(doc);
            }
        }

        return SearchResult.builder()
                .documents(page)
                .totalHits(totalHits)
                .offset(offset)
                .limit(limit)
                .searchTimeMs(totalTime)
                .build();
    }

    @Override
    public String getName() { return "rrf(k=" + k + ")"; }

    private String extractUid(JVS doc) {
        try {
            if (doc.exists("_uid")) return doc.getString("_uid");
            if (doc.exists("id.id")) return doc.getString("id.id");
        } catch (Exception ignored) {}
        return null;
    }
}
