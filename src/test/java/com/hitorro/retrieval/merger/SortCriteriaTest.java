/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.merger;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

class SortCriteriaTest {

    /** Simple extractor over Map<String, Object> — reads the requested
     *  field, returns null if missing. Keeps the test focused on the
     *  chain semantics, not extraction. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private final BiFunction<Map<String, Object>, String, Comparable<?>> extract =
            (m, f) -> (Comparable) m.get(f);

    @Test
    void singleKeyDescComparator() {
        SortCriteria c = SortCriteria.byField("date", SortCriteria.Direction.DESC);
        var items = List.of(Map.<String, Object>of("date", 100), Map.<String, Object>of("date", 50), Map.<String, Object>of("date", 200));
        var sorted = items.stream().sorted(c.asComparator(extract)).toList();
        assertThat(sorted).extracting(m -> m.get("date")).containsExactly(200, 100, 50);
    }

    @Test
    void singleKeyAscComparator() {
        SortCriteria c = SortCriteria.byField("date", SortCriteria.Direction.ASC);
        var items = List.of(Map.<String, Object>of("date", 100), Map.<String, Object>of("date", 50), Map.<String, Object>of("date", 200));
        var sorted = items.stream().sorted(c.asComparator(extract)).toList();
        assertThat(sorted).extracting(m -> m.get("date")).containsExactly(50, 100, 200);
    }

    @Test
    void compositeKeyFallsThroughOnTie() {
        // Primary date DESC, tiebreak by score DESC.
        SortCriteria c = SortCriteria.byField("date", SortCriteria.Direction.DESC)
                                     .then("score", SortCriteria.Direction.DESC);
        var items = List.of(
                Map.<String, Object>of("date", 100, "score", 1.0),
                Map.<String, Object>of("date", 100, "score", 5.0),
                Map.<String, Object>of("date", 100, "score", 3.0),
                Map.<String, Object>of("date", 200, "score", 1.0));
        var sorted = items.stream().sorted(c.asComparator(extract)).toList();
        // date 200 first, then within date 100: score 5.0, 3.0, 1.0
        assertThat(sorted).extracting(m -> m.get("date")).containsExactly(200, 100, 100, 100);
        assertThat(sorted.subList(1, 4)).extracting(m -> m.get("score"))
                                        .containsExactly(5.0, 3.0, 1.0);
    }

    @Test
    void nullsSortLastRegardlessOfDirection() {
        SortCriteria c = SortCriteria.byField("date", SortCriteria.Direction.ASC);
        java.util.HashMap<String, Object> nullMap = new java.util.HashMap<>();
        nullMap.put("date", null);
        var items = List.of(Map.<String, Object>of("date", 100), nullMap, Map.<String, Object>of("date", 50));
        var sorted = items.stream().sorted(c.asComparator(extract)).toList();
        // 50, 100, null — nulls-last on ASC
        assertThat(sorted.get(0).get("date")).isEqualTo(50);
        assertThat(sorted.get(1).get("date")).isEqualTo(100);
        assertThat(sorted.get(2).get("date")).isNull();

        // Same test with DESC — nulls STILL last (not first)
        SortCriteria d = SortCriteria.byField("date", SortCriteria.Direction.DESC);
        var descSorted = items.stream().sorted(d.asComparator(extract)).toList();
        assertThat(descSorted.get(0).get("date")).isEqualTo(100);
        assertThat(descSorted.get(1).get("date")).isEqualTo(50);
        assertThat(descSorted.get(2).get("date")).isNull();
    }

    @Test
    void threeKeyChainAppendsToTail() {
        // Build a 3-key chain via .then().then().
        SortCriteria c = SortCriteria.byField("a", SortCriteria.Direction.ASC)
                                     .then("b", SortCriteria.Direction.DESC)
                                     .then("c", SortCriteria.Direction.ASC);
        assertThat(c.field()).isEqualTo("a");
        assertThat(c.next().field()).isEqualTo("b");
        assertThat(c.next().next().field()).isEqualTo("c");
        assertThat(c.next().next().next()).isNull();
    }

    @Test
    void backwardCompatibleThreeArgConstructor() {
        // Old code that did `new SortCriteria(field, dir, type)` must still work.
        SortCriteria c = new SortCriteria("x", SortCriteria.Direction.ASC, SortCriteria.SortType.FIELD);
        assertThat(c.field()).isEqualTo("x");
        assertThat(c.next()).isNull();
    }
}
