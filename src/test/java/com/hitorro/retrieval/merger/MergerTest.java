package com.hitorro.retrieval.merger;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Result Mergers")
class MergerTest {

    private SearchResult makeResult(JVS... docs) {
        return SearchResult.builder()
                .documents(List.of(docs))
                .totalHits(docs.length)
                .searchTimeMs(10)
                .build();
    }

    private JVS doc(String uid, double score) {
        JVS d = new JVS();
        d.set("_uid", uid);
        d.set("_score", score);
        return d;
    }

    private JVS docWithField(String uid, double score, String field, String value) {
        JVS d = doc(uid, score);
        d.set(field, value);
        return d;
    }

    // ─── ScoreMerger ─────────────────────────────────────────────

    @Test
    @DisplayName("ScoreMerger sorts by score descending")
    void scoreMergerSortsByScore() {
        SearchResult r1 = makeResult(doc("a", 0.9), doc("b", 0.5));
        SearchResult r2 = makeResult(doc("c", 0.7), doc("d", 0.3));

        SearchResult merged = new ScoreMerger().merge(List.of(r1, r2), 0, 10);
        assertThat(merged.getDocuments()).hasSize(4);
        assertThat(merged.getDocuments().get(0).getString("_uid")).isEqualTo("a");
        assertThat(merged.getDocuments().get(1).getString("_uid")).isEqualTo("c");
    }

    @Test
    @DisplayName("ScoreMerger deduplicates by highest score")
    void scoreMergerDeduplicates() {
        SearchResult r1 = makeResult(doc("a", 0.9));
        SearchResult r2 = makeResult(doc("a", 0.5));

        SearchResult merged = new ScoreMerger().merge(List.of(r1, r2), 0, 10);
        assertThat(merged.getDocuments()).hasSize(1);
        assertThat(merged.getDocuments().get(0).getDouble("_score")).isEqualTo(0.9);
    }

    @Test
    @DisplayName("ScoreMerger respects offset and limit")
    void scoreMergerPagination() {
        SearchResult r1 = makeResult(doc("a", 0.9), doc("b", 0.8), doc("c", 0.7));
        SearchResult merged = new ScoreMerger().merge(List.of(r1), 1, 1);
        assertThat(merged.getDocuments()).hasSize(1);
        assertThat(merged.getDocuments().get(0).getString("_uid")).isEqualTo("b");
    }

    // ─── FieldSortMerger ─────────────────────────────────────────

    @Test
    @DisplayName("FieldSortMerger sorts by field ascending")
    void fieldSortMergerAsc() {
        SearchResult r1 = makeResult(docWithField("a", 1, "name", "Charlie"), docWithField("b", 1, "name", "Alice"));
        SearchResult r2 = makeResult(docWithField("c", 1, "name", "Bob"));

        SearchResult merged = new FieldSortMerger().merge(List.of(r1, r2), 0, 10,
                SortCriteria.byField("name", SortCriteria.Direction.ASC));
        assertThat(merged.getDocuments().get(0).getString("name")).isEqualTo("Alice");
        assertThat(merged.getDocuments().get(1).getString("name")).isEqualTo("Bob");
        assertThat(merged.getDocuments().get(2).getString("name")).isEqualTo("Charlie");
    }

    @Test
    @DisplayName("FieldSortMerger sorts descending")
    void fieldSortMergerDesc() {
        SearchResult r1 = makeResult(docWithField("a", 1, "name", "Alice"), docWithField("b", 1, "name", "Charlie"));
        SearchResult merged = new FieldSortMerger().merge(List.of(r1), 0, 10,
                SortCriteria.byField("name", SortCriteria.Direction.DESC));
        assertThat(merged.getDocuments().get(0).getString("name")).isEqualTo("Charlie");
    }

    // ─── RRFMerger ───────────────────────────────────────────────

    @Test
    @DisplayName("RRFMerger computes RRF scores correctly")
    void rrfMergerScores() {
        // doc "a" at rank 0 in both lists -> score = 2/(60+1) = 2/61
        SearchResult r1 = makeResult(doc("a", 0.9), doc("b", 0.5));
        SearchResult r2 = makeResult(doc("a", 0.8), doc("c", 0.3));

        SearchResult merged = new RRFMerger().merge(List.of(r1, r2), 0, 10);
        // "a" should be first (highest RRF score)
        assertThat(merged.getDocuments().get(0).getString("_uid")).isEqualTo("a");
        assertThat(merged.getDocuments()).hasSize(3); // a, b, c
    }

    @Test
    @DisplayName("RRFMerger handles disjoint results")
    void rrfMergerDisjoint() {
        SearchResult r1 = makeResult(doc("a", 0.9));
        SearchResult r2 = makeResult(doc("b", 0.8));

        SearchResult merged = new RRFMerger().merge(List.of(r1, r2), 0, 10);
        assertThat(merged.getDocuments()).hasSize(2);
        // Both have same RRF score (1/61 each), order depends on insertion
    }

    @Test
    @DisplayName("RRFMerger sums totalHits")
    void rrfMergerTotalHits() {
        SearchResult r1 = makeResult(doc("a", 0.9));
        SearchResult r2 = makeResult(doc("b", 0.8));

        SearchResult merged = new RRFMerger().merge(List.of(r1, r2), 0, 10);
        assertThat(merged.getTotalHits()).isEqualTo(2);
    }
}
