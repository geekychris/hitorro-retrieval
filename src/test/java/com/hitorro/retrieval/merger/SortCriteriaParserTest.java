/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.merger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers every input shape {@link SortCriteriaParser} accepts. The parser
 * is the single point of truth for sort-chain deserialisation on both the
 * BFF-facing wire (RetrievalController) and the pipeline-internal side
 * (IndexRetriever), so a regression here scrambles every layer.
 */
class SortCriteriaParserTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void nullAndBlankInputsReturnNull() {
        assertThat(SortCriteriaParser.parse(null)).isNull();
        assertThat(SortCriteriaParser.parseString(null)).isNull();
        assertThat(SortCriteriaParser.parseString("")).isNull();
        assertThat(SortCriteriaParser.parseString("   ")).isNull();
    }

    @Test
    void parseStringSingleKey() {
        SortCriteria c = SortCriteriaParser.parseString("date:asc");
        assertThat(c).isNotNull();
        assertThat(c.field()).isEqualTo("date");
        assertThat(c.direction()).isEqualTo(SortCriteria.Direction.ASC);
        assertThat(c.next()).isNull();
    }

    @Test
    void parseStringDefaultsUnknownDirectionToDesc() {
        SortCriteria c = SortCriteriaParser.parseString("date");
        assertThat(c.direction()).isEqualTo(SortCriteria.Direction.DESC);

        SortCriteria d = SortCriteriaParser.parseString("date:sideways");
        assertThat(d.direction()).isEqualTo(SortCriteria.Direction.DESC);
    }

    @Test
    void parseStringCompositeChain() {
        SortCriteria c = SortCriteriaParser.parseString("date:desc,_score:desc,title:asc");
        assertThat(fields(c)).containsExactly("date", "_score", "title");
        assertThat(directions(c)).containsExactly(
                SortCriteria.Direction.DESC,
                SortCriteria.Direction.DESC,
                SortCriteria.Direction.ASC);
    }

    @Test
    void parseArrayOfObjects() throws Exception {
        JsonNode arr = JSON.readTree("""
                [
                  { "field": "date",   "direction": "desc" },
                  { "field": "_score", "direction": "asc"  }
                ]
                """);
        SortCriteria c = SortCriteriaParser.parse(arr);
        assertThat(fields(c)).containsExactly("date", "_score");
        assertThat(directions(c)).containsExactly(
                SortCriteria.Direction.DESC,
                SortCriteria.Direction.ASC);
    }

    @Test
    void parseArraySkipsBlankFields() throws Exception {
        JsonNode arr = JSON.readTree("""
                [
                  { "field": "",    "direction": "desc" },
                  { "field": "date","direction": "asc"  }
                ]
                """);
        SortCriteria c = SortCriteriaParser.parse(arr);
        assertThat(fields(c)).containsExactly("date");
    }

    @Test
    void parseTextualJsonNodeGoesThroughStringPath() throws Exception {
        JsonNode textual = JSON.readTree("\"date:desc,_score:desc\"");
        SortCriteria c = SortCriteriaParser.parse(textual);
        assertThat(fields(c)).containsExactly("date", "_score");
    }

    @Test
    void parseUnexpectedShapeReturnsNull() throws Exception {
        // Objects and numbers aren't a sort spec — parser must not throw.
        assertThat(SortCriteriaParser.parse(JSON.readTree("42"))).isNull();
        assertThat(SortCriteriaParser.parse(JSON.readTree("{ \"foo\": 1 }"))).isNull();
    }

    // ---- helpers ----

    private static java.util.List<String> fields(SortCriteria head) {
        java.util.List<String> out = new java.util.ArrayList<>();
        for (SortCriteria n = head; n != null; n = n.next()) out.add(n.field());
        return out;
    }

    private static java.util.List<SortCriteria.Direction> directions(SortCriteria head) {
        java.util.List<SortCriteria.Direction> out = new java.util.ArrayList<>();
        for (SortCriteria n = head; n != null; n = n.next()) out.add(n.direction());
        return out;
    }
}
