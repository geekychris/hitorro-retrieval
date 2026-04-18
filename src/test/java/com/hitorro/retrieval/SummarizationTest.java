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

    /** Fake AI that returns canned responses */
    private final AIOperations fakeAI = new AIOperations() {
        public String translate(String text, String src, String tgt) { return "[translated]"; }
        public String summarize(String text, int maxWords) {
            return "Summary of " + maxWords + " words: key topics found.";
        }
        public String ask(String text, String question) { return "answer"; }
        public boolean isAvailable() { return true; }
    };

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
}
