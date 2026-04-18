package com.hitorro.retrieval;

import com.hitorro.index.IndexManager;
import com.hitorro.index.config.IndexConfig;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.RetrievalPipeline;
import com.hitorro.retrieval.pipeline.RetrievalPipelineBuilder;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

@DisplayName("Retrieval Pipeline")
class RetrievalPipelineTest {

    private IndexManager indexManager;

    @BeforeEach
    void setUp() throws Exception {
        indexManager = new IndexManager("en");
        IndexConfig config = IndexConfig.inMemory().build();
        indexManager.createIndex("test", config, null);

        // Index some test documents
        for (int i = 1; i <= 5; i++) {
            JVS doc = JVS.read("""
                    {"id": {"domain": "demo", "did": "doc-%d"}, "title": "Document %d about climate", "department": "Research"}
                    """.formatted(i, i));
            indexManager.indexDocument("test", doc);
        }
        for (int i = 6; i <= 10; i++) {
            JVS doc = JVS.read("""
                    {"id": {"domain": "demo", "did": "doc-%d"}, "title": "Document %d about technology", "department": "Engineering"}
                    """.formatted(i, i));
            indexManager.indexDocument("test", doc);
        }
        indexManager.commit("test");
    }

    @AfterEach
    void tearDown() throws Exception {
        indexManager.close();
    }

    @Test
    @DisplayName("Basic search returns documents and summary aggregate")
    void shouldSearchAndReturnDocuments() {
        RetrievalConfig config = new RetrievalConfig("test");
        RetrievalService service = new RetrievalService(indexManager);

        JVS query = JVS.read("""
                {"search": {"query": "*:*", "offset": 0, "limit": 10}}
                """);

        RetrievalResult result = service.retrieve(config, query);
        assertThat(result.hasErrors()).isFalse();

        List<JVS> docs = result.getDocumentList();
        assertThat(docs).hasSize(10);

        // Summary aggregate should be present
        List<JVS> aggregates = result.getAggregates();
        assertThat(aggregates).isNotEmpty();
        JVS summary = aggregates.stream()
                .filter(a -> "summary".equals(a.getString("_aggregate")))
                .findFirst().orElse(null);
        assertThat(summary).isNotNull();
    }

    @Test
    @DisplayName("Search with query filter returns subset")
    void shouldFilterByQuery() {
        RetrievalService service = new RetrievalService(indexManager);
        RetrievalConfig config = new RetrievalConfig("test");

        JVS query = JVS.read("""
                {"search": {"query": "title:climate", "limit": 20}}
                """);

        RetrievalResult result = service.retrieve(config, query);
        List<JVS> docs = result.getDocumentList();
        assertThat(docs).hasSizeLessThanOrEqualTo(5);
    }

    @Test
    @DisplayName("Search with limit constrains results")
    void shouldRespectLimit() {
        RetrievalService service = new RetrievalService(indexManager);
        RetrievalConfig config = new RetrievalConfig("test");

        JVS query = JVS.read("""
                {"search": {"query": "*:*", "limit": 3}}
                """);

        RetrievalResult result = service.retrieve(config, query);
        List<JVS> docs = result.getDocumentList();
        assertThat(docs).hasSize(3);
    }

    @Test
    @DisplayName("Pipeline builder requires IndexManager")
    void shouldFailWithoutIndexManager() {
        assertThatThrownBy(() -> new RetrievalPipelineBuilder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SearchProvider");
    }

    @Test
    @DisplayName("Custom stage is executed in pipeline")
    void shouldExecuteCustomStage() {
        RetrievalService service = new RetrievalService(indexManager);
        service.addCustomStage(new Retriever() {
            @Override
            public boolean participate(JVS query, RetrievalContext context) {
                return true;
            }

            @Override
            public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
                // Mark each document with a custom field
                return input.map(doc -> {
                    doc.set("_custom", "processed");
                    return doc;
                });
            }
        });

        RetrievalConfig config = new RetrievalConfig("test");
        JVS query = JVS.read("""
                {"search": {"query": "*:*", "limit": 5}}
                """);

        RetrievalResult result = service.retrieve(config, query);
        List<JVS> docs = result.getDocumentList();
        assertThat(docs).allSatisfy(doc ->
                assertThat(doc.getString("_custom")).isEqualTo("processed"));
    }

    @Test
    @DisplayName("Pipeline without search.query produces error")
    void shouldErrorWhenNoSearchQuery() {
        RetrievalService service = new RetrievalService(indexManager);
        RetrievalConfig config = new RetrievalConfig("test");

        JVS query = JVS.read("""
                {"other": "no search section"}
                """);

        RetrievalResult result = service.retrieve(config, query);
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors()).anyMatch(e -> e.contains("No pipeline stages participated"));
    }

    @Test
    @DisplayName("Pagination stage slices results")
    void shouldPaginate() {
        RetrievalConfig config = new RetrievalConfig("test");
        RetrievalService service = new RetrievalService(indexManager);

        // Request all 10 docs from index, then paginate client-side to page 1 of 3
        JVS query = JVS.read("""
                {"search": {"query": "*:*", "limit": 100}, "page": {"rows": 3, "page": 0}}
                """);

        RetrievalResult result = service.retrieve(config, query);
        List<JVS> docs = result.getDocumentList();
        assertThat(docs).hasSize(3);
    }

    @Test
    @DisplayName("RetrievalResult provides SearchResult access")
    void shouldExposeSearchResult() {
        RetrievalService service = new RetrievalService(indexManager);
        RetrievalConfig config = new RetrievalConfig("test");

        JVS query = JVS.read("""
                {"search": {"query": "*:*", "limit": 5}}
                """);

        RetrievalResult result = service.retrieve(config, query);
        SearchResult sr = result.getSearchResult();
        assertThat(sr).isNotNull();
        assertThat(sr.getTotalHits()).isEqualTo(10);
    }

    @Test
    @DisplayName("Pipeline stage count includes all registered stages")
    void shouldReportStageCount() {
        RetrievalPipeline pipeline = new RetrievalPipelineBuilder()
                .indexManager(indexManager)
                .disableFixup()
                .disablePagination()
                .disableFacets()
                .build();

        // Only IndexRetriever when everything else is disabled
        assertThat(pipeline.getStageCount()).isEqualTo(1);
    }
}
