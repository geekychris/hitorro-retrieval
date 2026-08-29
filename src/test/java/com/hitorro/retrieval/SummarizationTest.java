package com.hitorro.retrieval;

import com.hitorro.index.IndexManager;
import com.hitorro.index.config.IndexConfig;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.datamapper.AIOperations;
import com.hitorro.retrieval.context.ContextAttributes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Summarization Retriever")
class SummarizationTest {

    private IndexManager indexManager;

    /** Fake AI that returns canned responses AND records the last prompt seen by
     *  {@link #withPrompt} — used by the style/prompt tests to assert routing. */
    static class SpyAI implements AIOperations {
        volatile String lastPromptTemplate;
        volatile String lastText;
        public String translate(String text, String src, String tgt) { return "[translated]"; }
        public String summarize(String text, int maxWords) {
            lastPromptTemplate = null;      // legacy path — no template
            lastText = text;
            return "Summary of " + maxWords + " words: key topics found.";
        }
        public String ask(String text, String question) { return "answer"; }
        @Override public String withPrompt(String text, String promptTemplate, int maxWords) {
            lastPromptTemplate = promptTemplate;
            lastText = text;
            return "PROMPTED[" + (promptTemplate == null ? "-" : promptTemplate.substring(0, Math.min(24, promptTemplate.length()))) + "]";
        }
        public boolean isAvailable() { return true; }
    }
    private final SpyAI fakeAI = new SpyAI();

    @BeforeEach
    void setUp() throws Exception {
        indexManager = new IndexManager("en");
        IndexConfig config = IndexConfig.inMemory().build();
        indexManager.createIndex("test", config, null);
        for (int i = 1; i <= 5; i++) {
            JVS doc = JVS.read("{\"id\":{\"domain\":\"test\",\"did\":\"doc-" + i + "\"},"
                    + "\"title\":{\"mls\":[{\"lang\":\"en\",\"text\":\"Article " + i + " about AI\"}]},"
                    + "\"body\":{\"mls\":[{\"lang\":\"en\",\"text\":\"Body content for document " + i + "\"}]}}");
            indexManager.indexDocument("test", doc);
        }
        indexManager.commit("test");
    }

    @AfterEach
    void tearDown() throws Exception { indexManager.close(); }

    @Test
    @DisplayName("SummarizationRetriever produces aggregate when enabled")
    void shouldProduceSummaryAggregate() {
        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(fakeAI);

        JVS query = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":5},\"summarize\":{\"enabled\":true}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), query);

        List<JVS> docs = result.getDocumentList();
        assertThat(docs).hasSize(5); // documents pass through unchanged

        List<JVS> aggs = result.getAggregates();
        JVS summaryAgg = aggs.stream()
                .filter(a -> "ai_summary".equals(a.getString("_aggregate")))
                .findFirst().orElse(null);
        assertThat(summaryAgg).isNotNull();
        assertThat(summaryAgg.getString("summary")).contains("Summary of");
    }

    @Test
    @DisplayName("Summary stored in context attributes")
    void shouldStoreInContext() {
        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(fakeAI);

        JVS query = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":3},\"summarize\":{\"enabled\":true}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), query);
        result.getDocumentList(); // materialize

        String summary = result.getContext().getAttribute(ContextAttributes.AI_SUMMARY, String.class);
        assertThat(summary).isNotNull().contains("Summary");
    }

    @Test
    @DisplayName("No summarization when AI unavailable")
    void shouldSkipWhenAIUnavailable() {
        AIOperations offlineAI = new AIOperations() {
            public String translate(String t, String s, String tg) { return ""; }
            public String summarize(String t, int m) { return ""; }
            public String ask(String t, String q) { return ""; }
            public boolean isAvailable() { return false; }
        };

        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(offlineAI);

        JVS query = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":5},\"summarize\":{\"enabled\":true}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), query);
        List<JVS> docs = result.getDocumentList();
        assertThat(docs).hasSize(5);

        // No summary aggregate since AI is unavailable
        List<JVS> aggs = result.getAggregates();
        assertThat(aggs.stream().anyMatch(a -> "ai_summary".equals(a.getString("_aggregate")))).isFalse();
    }

    @Test
    @DisplayName("No summarization without summarize key in query")
    void shouldSkipWithoutQueryKey() {
        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(fakeAI);

        JVS query = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":5}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), query);
        result.getDocumentList();

        assertThat(result.getContext().hasAttribute(ContextAttributes.AI_SUMMARY)).isFalse();
    }

    @Test
    @DisplayName("summarize.style routes through withPrompt with the SummaryStyles template")
    void styleRoutesToWithPrompt() {
        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(fakeAI);

        JVS q = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":5},"
                + "\"summarize\":{\"style\":\"CONTRIBUTIONS\",\"maxWords\":200}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), q);
        result.getDocumentList();

        // Spy saw the CONTRIBUTIONS template (not the legacy summarize path).
        assertThat(fakeAI.lastPromptTemplate)
                .as("prompt template passed to withPrompt")
                .isNotNull()
                .contains("participant");
        JVS agg = result.getAggregates().stream()
                .filter(a -> "ai_summary".equals(a.getString("_aggregate")))
                .findFirst().orElseThrow();
        assertThat(agg.getString("summary")).startsWith("PROMPTED[");
        assertThat(agg.getString("style")).isEqualTo("CONTRIBUTIONS");
        assertThat(agg.getString("prompt")).isNotNull();
    }

    @Test
    @DisplayName("summarize.prompt overrides any style and is passed verbatim")
    void inlinePromptOverridesStyle() {
        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(fakeAI);

        String customPrompt = "Extract every mention of the word 'AI'.";
        JVS q = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":3},"
                + "\"summarize\":{\"style\":\"BRIEF\",\"prompt\":\"" + customPrompt + "\"}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), q);
        result.getDocumentList();

        assertThat(fakeAI.lastPromptTemplate).isEqualTo(customPrompt);
        JVS agg = result.getAggregates().stream()
                .filter(a -> "ai_summary".equals(a.getString("_aggregate")))
                .findFirst().orElseThrow();
        // Style tag on the aggregate reflects what the caller sent, even
        // though the prompt won — useful for auditing which slot fed the model.
        assertThat(agg.getString("style")).isEqualTo("BRIEF");
        assertThat(agg.getString("prompt")).isEqualTo(customPrompt);
    }

    @Test
    @DisplayName("Unknown style falls back to legacy summarize path with a warning")
    void unknownStyleFallsBackToLegacy() {
        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(fakeAI);

        JVS q = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":3},"
                + "\"summarize\":{\"style\":\"NOT_A_REAL_STYLE\"}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), q);
        result.getDocumentList();

        // Unknown → SummaryStyles.promptFor returns null → retriever falls
        // back to AIOperations.summarize (which the spy signals by leaving
        // lastPromptTemplate at null).
        assertThat(fakeAI.lastPromptTemplate).isNull();
        JVS agg = result.getAggregates().stream()
                .filter(a -> "ai_summary".equals(a.getString("_aggregate")))
                .findFirst().orElseThrow();
        assertThat(agg.getString("summary")).contains("Summary of");
        // Style is echoed verbatim so callers can distinguish "you asked for X
        // but I fell back" from "you asked for BRIEF".
        assertThat(agg.getString("style")).isEqualTo("NOT_A_REAL_STYLE");
    }

    @Test
    @DisplayName("Absent style + absent prompt uses legacy summarize path")
    void noStyleOrPromptUsesLegacyPath() {
        RetrievalService service = new RetrievalService(indexManager);
        service.enableSummarization(fakeAI);

        JVS q = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":2},\"summarize\":{}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), q);
        result.getDocumentList();

        assertThat(fakeAI.lastPromptTemplate).isNull();
        JVS agg = result.getAggregates().stream()
                .filter(a -> "ai_summary".equals(a.getString("_aggregate")))
                .findFirst().orElseThrow();
        assertThat(agg.getString("style")).isEqualTo("BRIEF");
    }
}
