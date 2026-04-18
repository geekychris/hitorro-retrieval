package com.hitorro.retrieval.merger;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;

import java.util.*;

/**
 * Merge-sorts results by a specified field value. Each source result is
 * assumed to be pre-sorted by the same field.
 */
public class FieldSortMerger implements ResultMerger {

    @Override
    public SearchResult merge(List<SearchResult> results, int offset, int limit) {
        return merge(results, offset, limit, SortCriteria.byScore());
    }

    @Override
    public SearchResult merge(List<SearchResult> results, int offset, int limit, SortCriteria criteria) {
        long totalHits = 0;
        long totalTime = 0;
        List<JVS> all = new ArrayList<>();

        for (SearchResult sr : results) {
            totalHits += sr.getTotalHits();
            totalTime = Math.max(totalTime, sr.getSearchTimeMs());
            all.addAll(sr.getDocuments());
        }

        Comparator<JVS> comparator = buildComparator(criteria);
        all.sort(comparator);

        int start = Math.min(offset, all.size());
        int end = Math.min(offset + limit, all.size());

        return SearchResult.builder()
                .documents(new ArrayList<>(all.subList(start, end)))
                .totalHits(totalHits)
                .offset(offset)
                .limit(limit)
                .searchTimeMs(totalTime)
                .build();
    }

    @Override
    public String getName() { return "field-sort"; }

    private Comparator<JVS> buildComparator(SortCriteria criteria) {
        Comparator<JVS> cmp = (a, b) -> {
            String va = getFieldValue(a, criteria.field());
            String vb = getFieldValue(b, criteria.field());
            if (va == null && vb == null) return 0;
            if (va == null) return 1;
            if (vb == null) return -1;
            // Try numeric comparison first
            try {
                return Double.compare(Double.parseDouble(va), Double.parseDouble(vb));
            } catch (NumberFormatException e) {
                return va.compareTo(vb);
            }
        };
        return criteria.direction() == SortCriteria.Direction.DESC ? cmp.reversed() : cmp;
    }

    private String getFieldValue(JVS doc, String field) {
        try {
            if (doc.exists(field)) return doc.getString(field);
        } catch (Exception ignored) {}
        return null;
    }
}
