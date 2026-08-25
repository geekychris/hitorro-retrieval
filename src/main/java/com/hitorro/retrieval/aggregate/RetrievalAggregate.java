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
 * A side-channel result produced by a pipeline stage.
 *
 * <p>Aggregates are collected alongside the main document stream and
 * materialized after iteration completes. Examples: search summary
 * (totalHits, timing), facet counts, cluster results.</p>
 *
 * <p>For federated searches, N per-source copies of the same aggregate
 * type get combined via {@link #mergeWith(List, MergePolicy, Map, RetrievalContext)}.
 * Each impl declares which policies it supports; the coordinator falls
 * back to {@link #defaultPolicy()} for unsupported values so
 * cross-source merges always produce something reasonable.</p>
 */
public interface RetrievalAggregate {

    /**
     * Materializes this aggregate as a JVS document.
     *
     * @param context the retrieval context (provides access to SearchResult, etc.)
     * @return a JVS document representing this aggregate, or null if nothing to report
     */
    JVS toJVS(RetrievalContext context);

    /**
     * Combine N per-source copies of this aggregate type into a single
     * merged aggregate. Called by the coordinator's cross-index merger
     * once every per-source pipeline has produced its aggregates.
     *
     * <p>The default implementation is a no-op (returns {@code this})
     * — matches the current {@link MergePolicy#PICK_FIRST} behaviour
     * and lets old aggregate types keep working without an update. Real
     * aggregates should override this and declare their supported
     * policies via {@link #supportedPolicies()}.</p>
     *
     * @param others   the OTHER per-source copies (excludes {@code this} —
     *                 the receiver is treated as the "first" source).
     * @param policy   caller-selected policy. Impls that don't support
     *                 this policy should fall back to their default
     *                 rather than throwing.
     * @param byIndex  source-name → per-source aggregate copy, including
     *                 {@code this}. Used to emit the {@code byIndex.<name>}
     *                 sub-object on the merged JVS.
     * @param context  the retrieval context (usually the merged one — some
     *                 aggregates need it to re-materialize, e.g. call
     *                 the summariser again for {@link MergePolicy#RESUMMARIZED}).
     * @return the merged aggregate — same runtime type as the receiver.
     */
    default RetrievalAggregate mergeWith(
            List<? extends RetrievalAggregate> others,
            MergePolicy policy,
            Map<String, ? extends RetrievalAggregate> byIndex,
            RetrievalContext context) {
        return this;
    }

    /**
     * The policy to fall back to when the coordinator hands {@link #mergeWith}
     * an unsupported / unknown policy. Kept as PICK_FIRST at the interface
     * level so unaware aggregate types stay silent-but-correct; real impls
     * override to something meaningful (SUMMED for numeric summaries,
     * PER_INDEX for text summaries, etc.).
     */
    default MergePolicy defaultPolicy() {
        return MergePolicy.PICK_FIRST;
    }

    /**
     * Which policies this aggregate type knows how to apply. Coordinator
     * uses this to (1) validate caller-supplied policies before merging
     * and (2) advertise the option set to consumers that render a
     * "merge as…" dropdown.
     */
    default Set<MergePolicy> supportedPolicies() {
        return EnumSet.of(defaultPolicy());
    }

    /**
     * Stable identifier for this aggregate type — used as the map key
     * when the coordinator wires a per-type policy overrides (i.e.
     * {@code mergePolicies: {"summary":"SUMMED","ai_summary":"PER_INDEX"}}).
     * Must match the {@code _aggregate} field value emitted in
     * {@link #toJVS(RetrievalContext)}.
     */
    default String aggregateName() {
        // Sensible default — camelCase class name minus "Aggregate" suffix.
        // Concrete aggregates override to match their JVS _aggregate value.
        String c = getClass().getSimpleName();
        if (c.endsWith("Aggregate")) c = c.substring(0, c.length() - "Aggregate".length());
        return c.isEmpty() ? getClass().getSimpleName() : Character.toLowerCase(c.charAt(0)) + c.substring(1);
    }
}
