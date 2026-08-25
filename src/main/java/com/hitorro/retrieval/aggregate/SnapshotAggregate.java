/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.aggregate;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

/**
 * A pre-materialized aggregate — holds a JVS produced by an earlier
 * step, hands it back verbatim from {@link #toJVS}. Used as the
 * output of {@link RetrievalAggregate#mergeWith}: concrete aggregates
 * (SearchSummary, Facet, Summarization) compute the merged JVS
 * eagerly during merge and wrap it as a SnapshotAggregate so the
 * coordinator emits it without needing to re-run stage logic.
 *
 * <p>Not intended for pipeline stages to construct directly — they
 * should register their real aggregate types (which know how to
 * merge). SnapshotAggregate is the merger's output shape.</p>
 */
public final class SnapshotAggregate implements RetrievalAggregate {

    private final String name;
    private final JVS baked;

    public SnapshotAggregate(String name, JVS baked) {
        this.name = name;
        this.baked = baked;
    }

    @Override
    public JVS toJVS(RetrievalContext context) {
        return baked;
    }

    @Override
    public String aggregateName() {
        return name;
    }
}
