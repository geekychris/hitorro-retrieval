/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.pipeline.stages;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SummaryStylesTest {

    @AfterEach
    void cleanupRuntimeRegistrations() {
        // Any test that adds a style should be responsible for its own removal
        // — belt-and-suspenders here so cross-test bleed can't cause flakes.
        SummaryStyles.unregister("TEST_STYLE");
        SummaryStyles.unregister("OVERRIDE_ME");
        SummaryStyles.unregister("case-insensitive");
    }

    @Test
    void built_in_styles_all_resolve_to_a_non_empty_prompt() {
        for (String name : new String[] {
                SummaryStyles.BRIEF, SummaryStyles.CONTRIBUTIONS, SummaryStyles.ACTION_ITEMS,
                SummaryStyles.DECISIONS, SummaryStyles.SENTIMENT, SummaryStyles.ENTITIES }) {
            String p = SummaryStyles.promptFor(name);
            assertThat(p).as(name + " prompt").isNotNull().isNotBlank();
        }
    }

    @Test
    void unknown_style_returns_null_not_an_exception() {
        assertThat(SummaryStyles.promptFor("NOT_A_STYLE")).isNull();
    }

    @Test
    void blank_and_null_are_null() {
        assertThat(SummaryStyles.promptFor(null)).isNull();
        assertThat(SummaryStyles.promptFor("")).isNull();
        assertThat(SummaryStyles.promptFor("   ")).isNull();
    }

    @Test
    void lookup_is_case_insensitive() {
        assertThat(SummaryStyles.promptFor("brief")).isEqualTo(SummaryStyles.promptFor("BRIEF"));
        assertThat(SummaryStyles.promptFor("Contributions")).isEqualTo(
                SummaryStyles.promptFor("CONTRIBUTIONS"));
    }

    @Test
    void runtime_register_makes_style_visible_and_case_normalized() {
        SummaryStyles.register("case-insensitive", "Do a thing.");
        assertThat(SummaryStyles.promptFor("CASE-INSENSITIVE")).isEqualTo("Do a thing.");
        assertThat(SummaryStyles.promptFor("case-insensitive")).isEqualTo("Do a thing.");
        assertThat(SummaryStyles.knownStyles()).contains("CASE-INSENSITIVE");
    }

    @Test
    void user_registration_overlays_built_in() {
        String originalBrief = SummaryStyles.promptFor("BRIEF");
        SummaryStyles.register("BRIEF", "OVERRIDDEN");
        try {
            assertThat(SummaryStyles.promptFor("BRIEF")).isEqualTo("OVERRIDDEN");
        } finally {
            // Undo the override so subsequent tests still see the built-in.
            SummaryStyles.unregister("BRIEF");
        }
        assertThat(SummaryStyles.promptFor("BRIEF")).isEqualTo(originalBrief);
    }

    @Test
    void unregister_only_removes_user_added_styles() {
        SummaryStyles.register("TEST_STYLE", "temp");
        assertThat(SummaryStyles.promptFor("TEST_STYLE")).isEqualTo("temp");
        SummaryStyles.unregister("TEST_STYLE");
        assertThat(SummaryStyles.promptFor("TEST_STYLE")).isNull();
        // Built-ins are untouched by unregister.
        SummaryStyles.unregister("BRIEF");
        assertThat(SummaryStyles.promptFor("BRIEF")).isNotNull();
    }

    @Test
    void knownStyles_lists_builtins_first_then_user_additions_in_order() {
        SummaryStyles.register("OVERRIDE_ME", "x");
        try {
            var list = SummaryStyles.knownStyles();
            assertThat(list).containsSubsequence(
                    SummaryStyles.BRIEF, SummaryStyles.CONTRIBUTIONS, SummaryStyles.ACTION_ITEMS);
            assertThat(list).contains("OVERRIDE_ME");
            assertThat(list.indexOf("OVERRIDE_ME"))
                    .isGreaterThan(list.indexOf(SummaryStyles.BRIEF));
        } finally { SummaryStyles.unregister("OVERRIDE_ME"); }
    }

    @Test
    void register_ignores_blank_input() {
        SummaryStyles.register(null, "x");
        SummaryStyles.register("", "x");
        SummaryStyles.register("VALID", null);
        assertThat(SummaryStyles.promptFor("")).isNull();
        assertThat(SummaryStyles.promptFor("VALID")).isNull();
    }
}
