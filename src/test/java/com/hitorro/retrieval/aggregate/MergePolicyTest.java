/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.aggregate;

import com.hitorro.index.search.FacetResult;
import com.hitorro.index.search.FacetResult.FacetValue;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MergePolicyTest {

    // ============================== SearchSummaryAggregate ==============================

    @Nested
    @DisplayName("SearchSummaryAggregate.mergeWith")
    class SummaryMerge {

        @Test
        void summedPolicySumsHitsAndMaxesTime() {
            SearchSummaryAggregate a = new SearchSummaryAggregate(fixture(60, 12));
            SearchSummaryAggregate b = new SearchSummaryAggregate(fixture(1000, 42));
            Map<String, SearchSummaryAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("mail-enriched-idx", a);
            byIdx.put("mail-jvs-enriched", b);

            RetrievalAggregate merged = a.mergeWith(List.of(b), MergePolicy.SUMMED, byIdx, null);
            JVS json = merged.toJVS(null);

            assertThat(json.getLong("totalHits")).isEqualTo(1060L);
            assertThat(json.getLong("searchTimeMs")).isEqualTo(42L);
            assertThat(json.exists("byIndex")).isTrue();
            assertThat(json.getLong("byIndex.mail-enriched-idx.totalHits")).isEqualTo(60L);
            assertThat(json.getLong("byIndex.mail-jvs-enriched.totalHits")).isEqualTo(1000L);
        }

        @Test
        void perIndexPolicyOmitsTopLevelTotals() {
            SearchSummaryAggregate a = new SearchSummaryAggregate(fixture(60, 12));
            SearchSummaryAggregate b = new SearchSummaryAggregate(fixture(1000, 42));
            Map<String, SearchSummaryAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", a);
            byIdx.put("B", b);

            JVS json = a.mergeWith(List.of(b), MergePolicy.PER_INDEX, byIdx, null).toJVS(null);
            assertThat(json.exists("totalHits")).isFalse();
            assertThat(json.exists("byIndex")).isTrue();
            assertThat(json.exists("byIndex.A")).isTrue();
            assertThat(json.exists("byIndex.B")).isTrue();
        }

        @Test
        void unsupportedPolicyFallsBackToDefault() {
            SearchSummaryAggregate a = new SearchSummaryAggregate(fixture(10, 5));
            Map<String, SearchSummaryAggregate> byIdx = Map.of("A", a);
            JVS json = a.mergeWith(List.of(), MergePolicy.CONCATENATED, byIdx, null).toJVS(null);
            // CONCATENATED unsupported → default SUMMED → totalHits present
            assertThat(json.exists("totalHits")).isTrue();
        }

        @Test
        void defaultPolicyAndSupportedPoliciesAdvertised() {
            SearchSummaryAggregate a = new SearchSummaryAggregate();
            assertThat(a.defaultPolicy()).isEqualTo(MergePolicy.SUMMED);
            assertThat(a.supportedPolicies()).containsExactlyInAnyOrder(
                    MergePolicy.SUMMED, MergePolicy.PER_INDEX);
            assertThat(a.aggregateName()).isEqualTo("summary");
        }

        private SearchResult fixture(long totalHits, long timeMs) {
            return SearchResult.builder()
                    .documents(List.of())
                    .totalHits(totalHits)
                    .query("*:*")
                    .offset(0)
                    .limit(20)
                    .searchTimeMs(timeMs)
                    .build();
        }
    }

    // ============================== FacetAggregate ==============================

    @Nested
    @DisplayName("FacetAggregate.mergeWith")
    class FacetMerge {

        @Test
        void summedPolicyUnionsCountsAcrossSources() {
            FacetAggregate a = new FacetAggregate(withFacet("sender_domain",
                    new FacetValue("redfin.com", 6), new FacetValue("substack.com", 3)));
            FacetAggregate b = new FacetAggregate(withFacet("sender_domain",
                    new FacetValue("redfin.com", 20), new FacetValue("temu.com", 8)));
            Map<String, FacetAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", a);
            byIdx.put("B", b);

            JVS json = a.mergeWith(List.of(b), MergePolicy.SUMMED, byIdx, null).toJVS(null);
            // Top-level dim present with summed counts
            assertThat(json.exists("sender_domain")).isTrue();
            // Both per-source breakdowns land in byIndex
            assertThat(json.exists("byIndex.A.sender_domain")).isTrue();
            assertThat(json.exists("byIndex.B.sender_domain")).isTrue();
        }

        @Test
        void perIndexPolicyOmitsTopLevelDims() {
            FacetAggregate a = new FacetAggregate(withFacet("sender_domain", new FacetValue("x", 1)));
            FacetAggregate b = new FacetAggregate(withFacet("sender_domain", new FacetValue("y", 2)));
            Map<String, FacetAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", a); byIdx.put("B", b);

            JVS json = a.mergeWith(List.of(b), MergePolicy.PER_INDEX, byIdx, null).toJVS(null);
            assertThat(json.exists("sender_domain")).isFalse();
            assertThat(json.exists("byIndex")).isTrue();
        }

        @Test
        void skipsSourcesWithNoFacetsButKeepsOthers() {
            FacetAggregate withF = new FacetAggregate(withFacet("dim", new FacetValue("v", 5)));
            FacetAggregate noF   = new FacetAggregate(SearchResult.builder()
                    .documents(List.of()).totalHits(0).build());   // no facets
            Map<String, FacetAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", withF); byIdx.put("B", noF);

            JVS json = withF.mergeWith(List.of(noF), MergePolicy.SUMMED, byIdx, null).toJVS(null);
            assertThat(json.exists("dim")).isTrue();
            assertThat(json.exists("byIndex.A")).isTrue();
            assertThat(json.exists("byIndex.B")).isFalse();   // no-facets source omitted
        }

        private SearchResult withFacet(String dim, FacetValue... values) {
            Map<String, FacetResult> facets = new LinkedHashMap<>();
            long total = 0;
            for (FacetValue v : values) total += v.getCount();
            facets.put(dim, new FacetResult(dim, List.of(values), total));
            return SearchResult.builder()
                    .documents(List.of())
                    .totalHits(total)
                    .facets(facets)
                    .build();
        }
    }

    // ============================== SummarizationAggregate ==============================

    @Nested
    @DisplayName("SummarizationAggregate.mergeWith")
    class SummarizationMerge {

        @Test
        void perIndexPolicyOmitsTopLevelSummary() {
            var a = new SummarizationAggregate("summary A", 10, 100);
            var b = new SummarizationAggregate("summary B", 20, 200);
            Map<String, SummarizationAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", a); byIdx.put("B", b);

            JVS json = a.mergeWith(List.of(b), MergePolicy.PER_INDEX, byIdx, null).toJVS(null);
            assertThat(json.exists("summary")).isFalse();
            assertThat(json.exists("byIndex.A.summary")).isTrue();
            assertThat(json.exists("byIndex.B.summary")).isTrue();
            assertThat(json.getLong("documentCount")).isEqualTo(30L);   // still summed
            assertThat(json.getLong("durationMs")).isEqualTo(300L);
        }

        @Test
        void concatenatedPolicyJoinsSummariesWithLabels() {
            var a = new SummarizationAggregate("emails about foo", 10, 100);
            var b = new SummarizationAggregate("emails about bar", 20, 200);
            Map<String, SummarizationAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", a); byIdx.put("B", b);

            JVS json = a.mergeWith(List.of(b), MergePolicy.CONCATENATED, byIdx, null).toJVS(null);
            String s = json.getString("summary");
            assertThat(s).contains("[A] emails about foo");
            assertThat(s).contains("[B] emails about bar");
        }

        @Test
        void pickFirstReturnsFirstSourceSummary() {
            var a = new SummarizationAggregate("first", 10, 100);
            var b = new SummarizationAggregate("second", 20, 200);
            Map<String, SummarizationAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", a); byIdx.put("B", b);

            JVS json = a.mergeWith(List.of(b), MergePolicy.PICK_FIRST, byIdx, null).toJVS(null);
            assertThat(json.getString("summary")).isEqualTo("first");
        }

        @Test
        void resummarizedDowngradesToConcatenatedInV1() {
            // No AIOperations wired yet — SummarizationAggregate should
            // silently downgrade RESUMMARIZED → CONCATENATED.
            var a = new SummarizationAggregate("x", 1, 1);
            var b = new SummarizationAggregate("y", 1, 1);
            Map<String, SummarizationAggregate> byIdx = new LinkedHashMap<>();
            byIdx.put("A", a); byIdx.put("B", b);

            JVS json = a.mergeWith(List.of(b), MergePolicy.RESUMMARIZED, byIdx, null).toJVS(null);
            assertThat(json.getString("summary")).contains("[A] x").contains("[B] y");
        }

        @Test
        void defaultPolicyAndSupportedPoliciesAdvertised() {
            var a = new SummarizationAggregate("x", 1, 1);
            assertThat(a.defaultPolicy()).isEqualTo(MergePolicy.PER_INDEX);
            assertThat(a.supportedPolicies())
                    .containsExactlyInAnyOrder(MergePolicy.PER_INDEX, MergePolicy.CONCATENATED,
                                               MergePolicy.RESUMMARIZED, MergePolicy.PICK_FIRST);
            assertThat(a.aggregateName()).isEqualTo("ai_summary");
        }
    }
}
