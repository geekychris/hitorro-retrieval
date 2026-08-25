/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.aggregate;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Aggregate containing an AI-generated summary of the search result set.
 *
 * <p>Merge policies (text-shaped, so the option set is different from
 * numeric aggregates):</p>
 * <ul>
 *   <li>{@link MergePolicy#PER_INDEX} (default) — emit only the
 *       {@code byIndex.<sourceName>} sub-object; top-level {@code summary}
 *       omitted. Each source's LLM output stays intact and labelled.</li>
 *   <li>{@link MergePolicy#CONCATENATED} — join per-source summaries
 *       with source-labelled delimiters into one {@code summary} string.
 *       No extra LLM tokens.</li>
 *   <li>{@link MergePolicy#RESUMMARIZED} — hand the concatenation back to
 *       the AI to produce ONE overall summary. Uses tokens on every
 *       federated search; NOT wired in v1 (falls back to CONCATENATED
 *       until an AIOperations bean lands on the merger's context).</li>
 *   <li>{@link MergePolicy#PICK_FIRST} — first source's summary wins;
 *       byIndex still carries everyone.</li>
 * </ul>
 */
public class SummarizationAggregate implements RetrievalAggregate {

    // Package-private so mergeWith can pull sibling fields without a getter zoo.
    final String summary;
    final int    documentCount;
    final long   durationMs;

    public SummarizationAggregate(String summary, int documentCount, long durationMs) {
        this.summary = summary;
        this.documentCount = documentCount;
        this.durationMs = durationMs;
    }

    @Override
    public String aggregateName() { return "ai_summary"; }

    @Override
    public MergePolicy defaultPolicy() { return MergePolicy.PER_INDEX; }

    @Override
    public Set<MergePolicy> supportedPolicies() {
        // RESUMMARIZED is advertised but currently degrades to CONCATENATED
        // because the merger context doesn't carry an AIOperations bean
        // in v1. Flip once that wiring exists.
        return EnumSet.of(MergePolicy.PER_INDEX, MergePolicy.CONCATENATED,
                          MergePolicy.RESUMMARIZED, MergePolicy.PICK_FIRST);
    }

    @Override
    public JVS toJVS(RetrievalContext context) {
        JVS jvs = new JVS();
        jvs.set("_aggregate", "ai_summary");
        jvs.set("summary", summary);
        jvs.set("documentCount", documentCount);
        jvs.set("durationMs", durationMs);
        return jvs;
    }

    @Override
    public RetrievalAggregate mergeWith(
            List<? extends RetrievalAggregate> others,
            MergePolicy policy,
            Map<String, ? extends RetrievalAggregate> byIndex,
            RetrievalContext context) {

        if (!supportedPolicies().contains(policy)) policy = defaultPolicy();
        // v1 downgrade — see supportedPolicies() comment.
        if (policy == MergePolicy.RESUMMARIZED) policy = MergePolicy.CONCATENATED;

        JVS merged = new JVS();
        merged.set("_aggregate", "ai_summary");

        JVS byIndexJson = new JVS();
        int totalDocs = 0;
        long totalDuration = 0;
        String firstSummary = null;
        StringBuilder concat = new StringBuilder();

        for (Map.Entry<String, ? extends RetrievalAggregate> e : byIndex.entrySet()) {
            RetrievalAggregate a = e.getValue();
            if (!(a instanceof SummarizationAggregate s)) continue;
            totalDocs += s.documentCount;
            totalDuration += s.durationMs;
            if (firstSummary == null) firstSummary = s.summary;

            if (concat.length() > 0) concat.append("\n\n");
            concat.append("[").append(e.getKey()).append("] ").append(s.summary == null ? "" : s.summary);

            JVS perSource = new JVS();
            perSource.set("summary",       s.summary);
            perSource.set("documentCount", s.documentCount);
            perSource.set("durationMs",    s.durationMs);
            // Unwrap nested JVS → JsonNode so JVS.set actually stores it.
            byIndexJson.set(e.getKey(), perSource.getJsonNode());
        }

        switch (policy) {
            case CONCATENATED -> merged.set("summary", concat.toString());
            case PICK_FIRST   -> merged.set("summary", firstSummary);
            case PER_INDEX    -> { /* only byIndex — no top-level summary */ }
            default           -> merged.set("summary", firstSummary);
        }

        merged.set("documentCount", totalDocs);
        merged.set("durationMs",    totalDuration);
        merged.set("byIndex",       byIndexJson.getJsonNode());
        return new SnapshotAggregate(aggregateName(), merged);
    }
}
