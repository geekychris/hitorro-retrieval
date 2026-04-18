package com.hitorro.retrieval;

import com.hitorro.retrieval.context.ContextAttributes;
import com.hitorro.retrieval.context.RetrievalContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Context Attributes")
class ContextAttributesTest {

    @Test
    void shouldSetAndGetTypedAttribute() {
        RetrievalContext ctx = new RetrievalContext("test", null, "en");
        ctx.setAttribute("key", "hello");
        assertThat(ctx.getAttribute("key", String.class)).isEqualTo("hello");
    }

    @Test
    void shouldReturnNullForMissingAttribute() {
        RetrievalContext ctx = new RetrievalContext("test", null, "en");
        assertThat(ctx.getAttribute("missing", String.class)).isNull();
    }

    @Test
    void shouldReturnEmptyOptionalForMissing() {
        RetrievalContext ctx = new RetrievalContext("test", null, "en");
        assertThat(ctx.getOptionalAttribute("missing", String.class)).isEmpty();
    }

    @Test
    void shouldReturnNullForWrongType() {
        RetrievalContext ctx = new RetrievalContext("test", null, "en");
        ctx.setAttribute("key", "hello");
        assertThat(ctx.getAttribute("key", Integer.class)).isNull();
    }

    @Test
    void shouldReportHasAttribute() {
        RetrievalContext ctx = new RetrievalContext("test", null, "en");
        ctx.setAttribute("key", 42);
        assertThat(ctx.hasAttribute("key")).isTrue();
        assertThat(ctx.hasAttribute("other")).isFalse();
    }

    @Test
    void shouldReturnUnmodifiableView() {
        RetrievalContext ctx = new RetrievalContext("test", null, "en");
        ctx.setAttribute("a", 1);
        assertThat(ctx.getAttributes()).containsKey("a");
    }

    @Test
    void predefinedKeysShouldBeNonNull() {
        assertThat(ContextAttributes.SORT_CRITERIA).isNotNull();
        assertThat(ContextAttributes.SEARCH_PROVIDERS).isNotNull();
        assertThat(ContextAttributes.AI_SUMMARY).isNotNull();
        assertThat(ContextAttributes.TOTAL_HITS).isNotNull();
    }
}
