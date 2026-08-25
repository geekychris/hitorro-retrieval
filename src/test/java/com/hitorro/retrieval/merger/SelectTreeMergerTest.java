/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.merger;

import com.hitorro.index.search.FacetResult;
import com.hitorro.index.search.FacetResult.FacetValue;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.aggregate.MergePolicy;
import com.hitorro.retrieval.stream.IteratorSearchStream;
import com.hitorro.retrieval.stream.SearchStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SelectTreeMergerTest {

    @Nested
    @DisplayName("doc phase — k-way merge on sort key")
    class DocPhase {

        @Test
        void twoSourcesScoreDesc() {
            SearchResult a = docsResult(doc("a1", 0.9), doc("a2", 0.5), doc("a3", 0.1));
            SearchResult b = docsResult(doc("b1", 0.8), doc("b2", 0.4));
            SelectTreeMerger m = new SelectTreeMerger(null, Map.of()); // default byScore()
            var out = m.merge(List.of(
                    new IteratorSearchStream("A", a, null),
                    new IteratorSearchStream("B", b, null)), 0, 10);
            assertThat(out.getDocuments()).extracting(d -> id(d))
                    .containsExactly("a1", "b1", "a2", "b2", "a3");
        }

        @Test
        void singleSourceIsPassThrough() {
            SearchResult a = docsResult(doc("x", 0.5));
            var out = new SelectTreeMerger(null, Map.of())
                    .merge(List.of(new IteratorSearchStream("A", a, null)), 0, 10);
            assertThat(out.getDocuments()).hasSize(1);
            assertThat(id(out.getDocuments().get(0))).isEqualTo("x");
        }

        @Test
        void emptySourcesReturnEmpty() {
            var out = new SelectTreeMerger(null, Map.of())
                    .merge(List.of(
                            new IteratorSearchStream("A", docsResult(), null),
                            new IteratorSearchStream("B", docsResult(), null)), 0, 10);
            assertThat(out.getDocuments()).isEmpty();
        }

        @Test
        void limitHonouredAcrossSources() {
            SearchResult a = docsResult(doc("a1", 0.9), doc("a2", 0.7), doc("a3", 0.5));
            SearchResult b = docsResult(doc("b1", 0.8), doc("b2", 0.6));
            var out = new SelectTreeMerger(null, Map.of())
                    .merge(List.of(
                            new IteratorSearchStream("A", a, null),
                            new IteratorSearchStream("B", b, null)), 0, 3);
            assertThat(out.getDocuments()).extracting(d -> id(d))
                    .containsExactly("a1", "b1", "a2");
        }

        @Test
        void sourceIndexProvenanceTagged() {
            var a = docsResult(doc("a1", 0.5));
            var b = docsResult(doc("b1", 0.9));
            var out = new SelectTreeMerger(null, Map.of())
                    .merge(List.of(
                            new IteratorSearchStream("indexA", a, null),
                            new IteratorSearchStream("indexB", b, null)), 0, 10);
            // First doc is b1 (higher score), tagged with indexB.
            JVS first = out.getDocuments().get(0);
            try {
                assertThat(first.getString(SelectTreeMerger.SOURCE_INDEX_FIELD)).isEqualTo("indexB");
            } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    @Nested
    @DisplayName("aggregate phase — park + drain + policy dispatch")
    class AggregatePhase {

        @Test
        void mergesSummaryAggregatesAcrossSources() {
            SearchResult a = withSummary(60, 12);
            SearchResult b = withSummary(1000, 42);
            var out = new SelectTreeMerger(null,
                    Map.of("summary", MergePolicy.SUMMED))
                    .merge(List.of(
                            new IteratorSearchStream("A", a, null),
                            new IteratorSearchStream("B", b, null)), 0, 10);
            assertThat(out.hasSourceAggregates()).isTrue();
            // Find the summary aggregate in the merged output.
            JVS summary = firstAggregate(out, "summary");
            try {
                assertThat(summary.getLong("totalHits")).isEqualTo(1060L);
                assertThat(summary.getLong("searchTimeMs")).isEqualTo(42L);
                assertThat(summary.exists("byIndex.A")).isTrue();
                assertThat(summary.exists("byIndex.B")).isTrue();
            } catch (Exception e) { throw new RuntimeException(e); }
        }

        @Test
        void mergesFacetAggregatesAcrossSources() {
            SearchResult a = withFacet("sender_domain", new FacetValue("redfin.com", 6));
            SearchResult b = withFacet("sender_domain", new FacetValue("redfin.com", 20),
                                                        new FacetValue("temu.com", 8));
            var out = new SelectTreeMerger(null,
                    Map.of("facets", MergePolicy.SUMMED))
                    .merge(List.of(
                            new IteratorSearchStream("A", a, null),
                            new IteratorSearchStream("B", b, null)), 0, 10);
            JVS facets = firstAggregate(out, "facets");
            assertThat(facets).isNotNull();
            try {
                assertThat(facets.exists("sender_domain")).isTrue();
                assertThat(facets.exists("byIndex.A")).isTrue();
                assertThat(facets.exists("byIndex.B")).isTrue();
            } catch (Exception e) { throw new RuntimeException(e); }
        }

        @Test
        void topLevelFacetsAlsoPopulatedForBackwardCompat() {
            SearchResult a = withFacet("dim", new FacetValue("x", 5));
            SearchResult b = withFacet("dim", new FacetValue("x", 3), new FacetValue("y", 4));
            var out = new SelectTreeMerger(null, Map.of())
                    .merge(List.of(
                            new IteratorSearchStream("A", a, null),
                            new IteratorSearchStream("B", b, null)), 0, 10);
            // getFacets() (existing accessor) must reflect summed union.
            assertThat(out.getFacets()).isNotNull().containsKey("dim");
            FacetResult dim = out.getFacets().get("dim");
            assertThat(dim.getValues()).extracting(FacetValue::getValue)
                    .containsExactlyInAnyOrder("x", "y");
        }
    }

    // ==================== helpers ============================================

    private static SearchResult docsResult(JVS... docs) {
        return SearchResult.builder()
                .documents(new ArrayList<>(List.of(docs)))
                .totalHits(docs.length)
                .build();
    }

    private static SearchResult withSummary(long totalHits, long timeMs) {
        return SearchResult.builder()
                .documents(new ArrayList<>())
                .totalHits(totalHits)
                .searchTimeMs(timeMs)
                .query("*:*")
                .offset(0)
                .limit(20)
                .build();
    }

    private static SearchResult withFacet(String dim, FacetValue... values) {
        Map<String, FacetResult> facets = new LinkedHashMap<>();
        long total = 0;
        for (FacetValue v : values) total += v.getCount();
        facets.put(dim, new FacetResult(dim, List.of(values), total));
        return SearchResult.builder()
                .documents(new ArrayList<>())
                .totalHits(total)
                .facets(facets)
                .build();
    }

    private static JVS doc(String id, double score) {
        JVS d = new JVS();
        try {
            d.set("_id", id);
            d.set("_score", score);
        } catch (Exception e) { throw new RuntimeException(e); }
        return d;
    }

    private static String id(JVS d) {
        try { return d.exists("_id") ? d.getString("_id") : null; }
        catch (Exception e) { return null; }
    }

    private static JVS firstAggregate(SearchResult sr, String name) {
        if (!sr.hasSourceAggregates()) return null;
        for (JVS agg : sr.getSourceAggregates()) {
            try {
                if (agg.exists("_aggregate") && name.equals(agg.getString("_aggregate"))) return agg;
            } catch (Exception ignore) { }
        }
        return null;
    }
}
