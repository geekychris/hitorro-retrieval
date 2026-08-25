/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.stream;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.aggregate.RetrievalAggregate;

/**
 * A single frame emitted by a {@link SearchStream}. Sealed union of
 * three shapes:
 *
 * <ul>
 *   <li>{@link DocFrame} — one search result document. The k-way
 *       merger orders these by the caller-supplied comparator.</li>
 *   <li>{@link AggregateFrame} — one aggregate produced by the
 *       source's pipeline (summary, facets, ai_summary, …). Emitted
 *       AFTER the source's DocFrames. The merger parks the source
 *       when it hits its first AggregateFrame + stashes further
 *       frames from that source for post-doc combination.</li>
 *   <li>{@link Eos} — end-of-stream marker. Sources emit this after
 *       their last frame. Presence lets the merger tell "no more from
 *       this source" from "next frame not yet ready".</li>
 * </ul>
 *
 * <p>Sealed to keep merge-logic pattern-matches exhaustive — adding a
 * new frame type is a compile-time fan-out through every consumer.</p>
 */
public sealed interface StreamFrame {

    /** A search result document from the source's per-index pipeline. */
    record DocFrame(JVS doc) implements StreamFrame { }

    /** An aggregate (summary / facets / ai_summary / …) from the
     *  source's per-index pipeline. Merger consumes these after
     *  DocFrames are drained. */
    record AggregateFrame(RetrievalAggregate aggregate) implements StreamFrame { }

    /** End-of-stream marker. Terminal — no frames follow. */
    record Eos() implements StreamFrame {
        public static final Eos INSTANCE = new Eos();
    }
}
