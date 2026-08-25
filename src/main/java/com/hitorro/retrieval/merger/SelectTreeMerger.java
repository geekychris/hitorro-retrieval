/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.merger;

import com.hitorro.index.search.FacetResult;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.aggregate.MergePolicy;
import com.hitorro.retrieval.aggregate.RetrievalAggregate;
import com.hitorro.retrieval.stream.SearchStream;
import com.hitorro.retrieval.stream.StreamFrame;
import com.hitorro.retrieval.stream.StreamFrame.AggregateFrame;
import com.hitorro.retrieval.stream.StreamFrame.DocFrame;
import com.hitorro.retrieval.stream.StreamFrame.Eos;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * K-way streaming merger over per-source {@link SearchStream}s.
 *
 * <p>State machine (per merge invocation):</p>
 * <ol>
 *   <li><b>Doc phase</b> — maintain a {@link PriorityQueue} of "live"
 *       cursors, ordered by the head DocFrame's sort key (from the
 *       caller-supplied {@link SortCriteria} chain). Poll head, emit
 *       its doc, advance the cursor, re-insert if it has more docs.
 *       When a cursor's next frame is an {@link AggregateFrame}, PARK
 *       it — remove from the PQ + stash the cursor for phase 2.</li>
 *   <li><b>Aggregate phase</b> — drain each parked cursor's remaining
 *       AggregateFrames. Group by aggregate type
 *       ({@link RetrievalAggregate#aggregateName()}), look up the
 *       type's policy in {@code policies} (fall back to the aggregate's
 *       {@link RetrievalAggregate#defaultPolicy()}), call
 *       {@link RetrievalAggregate#mergeWith}. Emit merged aggregates
 *       in the canonical order they were first seen.</li>
 * </ol>
 *
 * <p>Merger emits a materialized {@link SearchResult} today (docs +
 * facets from summed FacetAggregate + sourceAggregates list carrying
 * every merged aggregate). Truly-streaming output (yield frames back
 * to the caller as they merge) is a follow-up — the internal machine
 * is already streaming, so it's a matter of exposing a StreamFrame
 * iterator interface if a caller needs one.</p>
 */
public final class SelectTreeMerger {

    /** Provenance field written onto every merged doc so consumers
     *  can see which source it came from. Additive; no existing
     *  consumer parses this. */
    public static final String SOURCE_INDEX_FIELD = "_sourceIndex";

    private final Comparator<JVS> docComparator;
    private final Map<String, MergePolicy> policies;

    /**
     * @param sortCriteria drives the doc-frame priority queue. Null
     *                     falls back to {@link SortCriteria#byScore()}.
     * @param policies     aggregate-name → policy override. Missing
     *                     keys use each aggregate's default.
     */
    public SelectTreeMerger(SortCriteria sortCriteria, Map<String, MergePolicy> policies) {
        SortCriteria criteria = sortCriteria != null ? sortCriteria : SortCriteria.byScore();
        this.docComparator = criteria.asComparator(SelectTreeMerger::extractSortValue);
        this.policies = policies == null ? Map.of() : policies;
    }

    /** Merge N streams into one materialized SearchResult. */
    public SearchResult merge(List<SearchStream> streams, int offset, int limit) {
        List<JVS> mergedDocs = new ArrayList<>();
        Map<String, List<RetrievalAggregate>> parkedAggs = new LinkedHashMap<>();
        Map<String, SearchStream> parkedStreams = new LinkedHashMap<>();

        // Cursor state — each active source has its peeked head + a
        // pointer to the stream. PQ ordered by the head DocFrame's key.
        PriorityQueue<Cursor> active = new PriorityQueue<>((a, b) ->
                docComparator.compare(unwrap(a.head), unwrap(b.head)));

        // Seed cursors: peek the first frame of every stream. Sources
        // whose first frame is already an AggregateFrame or Eos never
        // enter the active PQ.
        for (SearchStream s : streams) {
            StreamFrame first = s.hasNext() ? s.next() : Eos.INSTANCE;
            if (first instanceof DocFrame) {
                active.offer(new Cursor(s, first));
            } else if (first instanceof AggregateFrame af) {
                park(s, af, parkedStreams, parkedAggs);
            }
            // Eos → discard.
        }

        // ---------- Doc phase ----------
        int total = offset + limit;
        while (!active.isEmpty() && mergedDocs.size() < total) {
            Cursor head = active.poll();
            DocFrame doc = (DocFrame) head.head;
            // Tag with source index for provenance. Emit only when past
            // offset; skipped docs still consumed for correct pagination.
            JVS tagged = doc.doc();
            try { tagged.set(SOURCE_INDEX_FIELD, head.stream.sourceName()); }
            catch (Exception ignore) { /* if id is composite / immutable, skip tag */ }
            if (mergedDocs.size() >= offset || total <= limit) {   // include when offset==0
                mergedDocs.add(tagged);
            }

            // Advance the cursor. Peek next frame; re-insert if doc,
            // park if aggregate, drop if Eos.
            StreamFrame nxt = head.stream.hasNext() ? head.stream.next() : Eos.INSTANCE;
            if (nxt instanceof DocFrame) {
                head.head = nxt;
                active.offer(head);
            } else if (nxt instanceof AggregateFrame af) {
                park(head.stream, af, parkedStreams, parkedAggs);
            }
            // Eos → cursor drops out silently.
        }

        // Drain remaining active cursors' aggregates (they still have
        // docs we skipped past the limit — advance until we hit Agg or Eos).
        while (!active.isEmpty()) {
            Cursor c = active.poll();
            StreamFrame f = c.head;
            while (!(f instanceof AggregateFrame) && !(f instanceof Eos)) {
                if (!c.stream.hasNext()) { f = Eos.INSTANCE; break; }
                f = c.stream.next();
            }
            if (f instanceof AggregateFrame af) park(c.stream, af, parkedStreams, parkedAggs);
        }

        // ---------- Aggregate phase ----------
        // Continue draining parked streams — an AggregateFrame was seen
        // to park them; keep pulling until Eos.
        for (Map.Entry<String, SearchStream> e : parkedStreams.entrySet()) {
            SearchStream s = e.getValue();
            while (s.hasNext()) {
                StreamFrame f = s.next();
                if (f instanceof AggregateFrame af) {
                    parkedAggs.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(af.aggregate());
                } else if (f instanceof Eos) break;
                // DocFrames post-aggregate shouldn't happen; if they do, drop.
            }
        }

        // Group aggregates by name across sources, merge each type.
        List<JVS> mergedAggregates = new ArrayList<>();
        Map<String, RetrievalAggregate> firstOfType = new LinkedHashMap<>();
        Map<String, LinkedHashMap<String, RetrievalAggregate>> byName_byIndex = new LinkedHashMap<>();
        for (Map.Entry<String, List<RetrievalAggregate>> e : parkedAggs.entrySet()) {
            String sourceIdx = e.getKey();
            for (RetrievalAggregate a : e.getValue()) {
                String name = a.aggregateName();
                firstOfType.putIfAbsent(name, a);
                byName_byIndex.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(sourceIdx, a);
            }
        }
        for (Map.Entry<String, RetrievalAggregate> ft : firstOfType.entrySet()) {
            RetrievalAggregate first = ft.getValue();
            Map<String, RetrievalAggregate> byIdx = byName_byIndex.get(ft.getKey());
            List<RetrievalAggregate> others = new ArrayList<>(byIdx.values());
            others.remove(first);
            MergePolicy policy = policies.getOrDefault(ft.getKey(), first.defaultPolicy());
            RetrievalAggregate merged = first.mergeWith(others, policy, byIdx, null);
            JVS jvs = merged.toJVS(null);
            if (jvs != null) mergedAggregates.add(jvs);
        }

        // Close every stream (parked + already-drained).
        for (SearchStream s : streams) {
            try { s.close(); } catch (Exception ignored) { }
        }

        // Slice for offset when relevant (we tolerated overshoot above).
        int start = Math.min(offset, mergedDocs.size());
        int end   = Math.min(offset + limit, mergedDocs.size());
        List<JVS> page = new ArrayList<>(mergedDocs.subList(start, end));

        return SearchResult.builder()
                .documents(page)
                .totalHits(totalHitsFromAggregates(byName_byIndex))
                .facets(facetsFromAggregates(byName_byIndex))
                .offset(offset)
                .limit(limit)
                .sourceAggregates(mergedAggregates)
                .build();
    }

    // ==================== helpers ============================================

    private static void park(SearchStream s, AggregateFrame first,
                             Map<String, SearchStream> parkedStreams,
                             Map<String, List<RetrievalAggregate>> parkedAggs) {
        parkedStreams.put(s.sourceName(), s);
        parkedAggs.computeIfAbsent(s.sourceName(), k -> new ArrayList<>()).add(first.aggregate());
    }

    /** Extract the sort value for a field from a doc. Special-cases
     *  {@code _score} to a numeric read; otherwise falls through to
     *  the JVS path get + a naive to-comparable coercion. Nulls flow
     *  through as null (nulls-last handled by {@link SortCriteria}). */
    @SuppressWarnings("rawtypes")
    private static Comparable extractSortValue(JVS doc, String field) {
        if (doc == null) return null;
        if ("_score".equals(field)) {
            try { return doc.exists("_score") ? doc.getDouble("_score") : 0.0; }
            catch (Exception ignore) { return 0.0; }
        }
        try {
            if (!doc.exists(field)) return null;
            var node = doc.get(field);
            if (node == null || node.isNull()) return null;
            if (node.isNumber()) return node.asDouble();
            if (node.isBoolean()) return node.asBoolean();
            return node.asText();
        } catch (Exception e) {
            return null;
        }
    }

    private static JVS unwrap(StreamFrame f) {
        return f instanceof DocFrame df ? df.doc() : null;
    }

    /** Pull the summed totalHits out of the summary aggregate, if any.
     *  Falls back to zero for single-doc-only merges. */
    private static long totalHitsFromAggregates(
            Map<String, LinkedHashMap<String, RetrievalAggregate>> byName_byIndex) {
        LinkedHashMap<String, RetrievalAggregate> summaries = byName_byIndex.get("summary");
        if (summaries == null) return 0;
        long sum = 0;
        for (RetrievalAggregate a : summaries.values()) {
            // We only have the source SearchResult when it's a raw
            // SearchSummaryAggregate — the SnapshotAggregate merger
            // output would have already summed, but we don't get here
            // from that path. Sum directly from source SearchResults.
            if (a instanceof com.hitorro.retrieval.aggregate.SearchSummaryAggregate s) {
                try {
                    JVS jvs = s.toJVS(null);
                    if (jvs != null && jvs.exists("totalHits")) sum += jvs.getLong("totalHits");
                } catch (Exception ignore) { }
            }
        }
        return sum;
    }

    /** Extract summed facets from the merged FacetAggregate for the
     *  SearchResult top-level facet slot — keeps existing consumers
     *  that read {@code result.getFacets()} working without changes. */
    @SuppressWarnings("unchecked")
    private static Map<String, FacetResult> facetsFromAggregates(
            Map<String, LinkedHashMap<String, RetrievalAggregate>> byName_byIndex) {
        LinkedHashMap<String, RetrievalAggregate> facetAggs = byName_byIndex.get("facets");
        if (facetAggs == null || facetAggs.isEmpty()) return null;
        // Gather per-source SearchResults; reuse ResultMerger.mergeFacets.
        List<SearchResult> srs = new ArrayList<>();
        for (RetrievalAggregate a : facetAggs.values()) {
            if (a instanceof com.hitorro.retrieval.aggregate.FacetAggregate f) {
                java.lang.reflect.Field cap;
                try {
                    cap = com.hitorro.retrieval.aggregate.FacetAggregate.class.getDeclaredField("captured");
                    cap.setAccessible(true);
                    Object sr = cap.get(f);
                    if (sr instanceof SearchResult r) srs.add(r);
                } catch (Exception ignore) { /* skip */ }
            }
        }
        return srs.isEmpty() ? null : ResultMerger.mergeFacets(srs);
    }

    // ==================== inner types ========================================

    private static final class Cursor {
        final SearchStream stream;
        StreamFrame head;   // always a DocFrame while cursor is in the active PQ.
        Cursor(SearchStream stream, StreamFrame head) { this.stream = stream; this.head = head; }
    }
}
