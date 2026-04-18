package com.hitorro.retrieval.aggregate;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

/**
 * Aggregate containing an AI-generated summary of the search result set.
 */
public class SummarizationAggregate implements RetrievalAggregate {

    private final String summary;
    private final int documentCount;
    private final long durationMs;

    public SummarizationAggregate(String summary, int documentCount, long durationMs) {
        this.summary = summary;
        this.documentCount = documentCount;
        this.durationMs = durationMs;
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
}
