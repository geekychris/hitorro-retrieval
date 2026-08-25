/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.merger;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Wire-shape parsers for {@link SortCriteria} chains — the same syntax
 * every layer accepts on the way in: the fleet-retrieval controller,
 * the {@code IndexRetriever} pipeline stage, and the search-ui BFF.
 *
 * <p>Two input shapes are supported:</p>
 * <ul>
 *   <li><b>Array of objects</b> — {@code [{"field":"date","direction":"desc"},
 *       {"field":"_score","direction":"desc"}]}. This is the canonical
 *       form the coordinator emits on the wire and the shape callers
 *       should prefer when composing programmatically.</li>
 *   <li><b>Legacy string</b> — {@code "date:desc,_score:desc"}. Kept
 *       for compact URLs / BFF pass-through. Both single-key
 *       ({@code "date:desc"}) and multi-key comma-joined forms parse.</li>
 * </ul>
 *
 * <p>Unknown directions default to {@code DESC} — the historic behaviour
 * of every ad-hoc parser this consolidates. Blank / null / empty inputs
 * return {@code null}, letting the caller apply its own default (usually
 * {@link SortCriteria#byScore()}).</p>
 *
 * <p>Kept in the {@code merger} package next to {@link SortCriteria}
 * itself so any module that already depends on the sort chain can parse
 * it without pulling in the coordinator or the BFF.</p>
 */
public final class SortCriteriaParser {

    private SortCriteriaParser() { }

    /**
     * Parse a JSON node into a {@link SortCriteria} chain. Accepts an
     * array of {@code {field, direction}} objects, a comma-joined
     * {@code "field:direction[,…]"} string, or null / missing (returns
     * null).
     */
    public static SortCriteria parse(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return null;
        if (node.isArray())   return parseArray(node);
        if (node.isTextual()) return parseString(node.asText());
        return null;
    }

    /**
     * Parse a comma-joined {@code "field:direction[,field:direction…]"}
     * string. Blanks / null return null. Direction defaults to
     * {@code DESC} when omitted or unrecognised.
     */
    public static SortCriteria parseString(String s) {
        if (s == null || s.isBlank()) return null;
        SortCriteria head = null;
        for (String seg : s.split(",")) {
            String trimmed = seg.trim();
            if (trimmed.isEmpty()) continue;
            String[] parts = trimmed.split(":");
            if (parts.length == 0 || parts[0].isBlank()) continue;
            SortCriteria.Direction dir = parts.length > 1 && "asc".equalsIgnoreCase(parts[1])
                    ? SortCriteria.Direction.ASC
                    : SortCriteria.Direction.DESC;
            head = (head == null) ? SortCriteria.byField(parts[0], dir) : head.then(parts[0], dir);
        }
        return head;
    }

    private static SortCriteria parseArray(JsonNode arr) {
        SortCriteria head = null;
        for (JsonNode elem : arr) {
            String field = elem.path("field").asText(null);
            if (field == null || field.isBlank()) continue;
            SortCriteria.Direction dir = "asc".equalsIgnoreCase(elem.path("direction").asText("desc"))
                    ? SortCriteria.Direction.ASC
                    : SortCriteria.Direction.DESC;
            head = (head == null) ? SortCriteria.byField(field, dir) : head.then(field, dir);
        }
        return head;
    }
}
