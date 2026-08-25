/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.merger;

import java.util.Comparator;
import java.util.function.BiFunction;

/**
 * Describes how search results should be sorted when merging from multiple providers.
 *
 * <p>Composite sort keys are expressed as a linked chain via {@link #next()}
 * — {@code SortCriteria.byField("date", DESC).then("_score", DESC)} produces
 * a two-key sort that orders by {@code date} first and only falls back
 * to {@code _score} when the primary-key values tie. The chain terminates
 * with {@code next() == null}.</p>
 *
 * <p>{@link #asComparator(BiFunction)} materializes the chain as a
 * {@link Comparator} — hand it any (item, fieldName) → value extractor
 * and pass the comparator to the streaming merger's priority queue.</p>
 */
public record SortCriteria(String field, Direction direction, SortType type, SortCriteria next) {

    public enum Direction { ASC, DESC }
    public enum SortType { SCORE, FIELD, CUSTOM }

    // ---------- Backward-compat single-key factories ----------

    public static SortCriteria byScore() {
        return new SortCriteria("_score", Direction.DESC, SortType.SCORE, null);
    }

    public static SortCriteria byField(String field, Direction direction) {
        return new SortCriteria(field, direction, SortType.FIELD, null);
    }

    /** Legacy three-arg constructor for callers that don't compose keys. */
    public SortCriteria(String field, Direction direction, SortType type) {
        this(field, direction, type, null);
    }

    // ---------- Chain builder ----------

    /**
     * Append another sort key onto the tail of this chain. Reads as
     * {@code SortCriteria.byField("date", DESC).then("_score", DESC)} —
     * primary by date, tiebreak by score.
     */
    public SortCriteria then(String field, Direction direction, SortType type) {
        SortCriteria appended = new SortCriteria(field, direction, type, null);
        return appendTail(this, appended);
    }

    public SortCriteria then(String field, Direction direction) {
        return then(field, direction, SortType.FIELD);
    }

    private static SortCriteria appendTail(SortCriteria head, SortCriteria toAppend) {
        if (head.next == null) {
            return new SortCriteria(head.field, head.direction, head.type, toAppend);
        }
        return new SortCriteria(head.field, head.direction, head.type,
                appendTail(head.next, toAppend));
    }

    // ---------- Comparator materialization ----------

    /**
     * Turn this chain into a {@link Comparator}. Caller supplies an
     * extractor that, given an item and a field name, returns the
     * comparable value for that field. Extractor return type is
     * {@code Comparable} — supports numbers, strings, dates.
     *
     * <p>Ties on the primary key fall through to {@link #next()}; ties
     * on the tail return 0. Nulls sort last regardless of direction.</p>
     *
     * @param extract (item, fieldName) → comparable value (or null)
     */
    public <T> Comparator<T> asComparator(BiFunction<T, String, Comparable<?>> extract) {
        return (a, b) -> compareChain(this, a, b, extract);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> int compareChain(SortCriteria node, T a, T b,
                                        BiFunction<T, String, Comparable<?>> extract) {
        if (node == null) return 0;
        Comparable va = extract.apply(a, node.field);
        Comparable vb = extract.apply(b, node.field);

        // Nulls last regardless of direction — a null head is "worse"
        // than any value for both ASC and DESC (we're merging top-N).
        if (va == null && vb == null) return compareChain(node.next, a, b, extract);
        if (va == null) return 1;
        if (vb == null) return -1;

        int c;
        try { c = va.compareTo(vb); }
        catch (ClassCastException e) {
            // Fallback to string compare — different value types on the
            // same field across sources shouldn't crash the merge.
            c = String.valueOf(va).compareTo(String.valueOf(vb));
        }
        if (node.direction == Direction.DESC) c = -c;
        return c == 0 ? compareChain(node.next, a, b, extract) : c;
    }
}
