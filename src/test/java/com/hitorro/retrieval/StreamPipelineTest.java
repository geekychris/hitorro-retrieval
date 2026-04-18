package com.hitorro.retrieval;

import com.hitorro.index.IndexManager;
import com.hitorro.index.config.IndexConfig;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.RetrievalPipelineBuilder;
import com.hitorro.retrieval.pipeline.StreamRetriever;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Stream Pipeline")
class StreamPipelineTest {

    private IndexManager indexManager;

    @BeforeEach
    void setUp() throws Exception {
        indexManager = new IndexManager("en");
        IndexConfig config = IndexConfig.inMemory().build();
        indexManager.createIndex("test", config, null);
        for (int i = 1; i <= 5; i++) {
            JVS doc = JVS.read("{\"id\":{\"domain\":\"test\",\"did\":\"doc-" + i + "\"},\"title\":\"Doc " + i + "\"}");
            indexManager.indexDocument("test", doc);
        }
        indexManager.commit("test");
    }

    @AfterEach
    void tearDown() throws Exception { indexManager.close(); }

    @Test
    @DisplayName("StreamRetriever stage filters via Stream API")
    void streamRetrieverFilters() {
        StreamRetriever filter = new StreamRetriever() {
            public boolean participate(JVS query, RetrievalContext ctx) { return true; }
            public Stream<JVS> retrieve(Stream<JVS> input, JVS query, RetrievalContext ctx) {
                return input.filter(doc -> {
                    try { return doc.getString("title").contains("1") || doc.getString("title").contains("3"); }
                    catch (Exception e) { return false; }
                });
            }
        };

        RetrievalService service = new RetrievalService(indexManager);
        service.addCustomStage(new com.hitorro.retrieval.pipeline.StreamRetrieverAdapter(filter));

        JVS query = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":10}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), query);
        List<JVS> docs = result.getDocumentList();
        assertThat(docs).hasSize(2);
    }

    @Test
    @DisplayName("RetrievalResult provides document stream")
    void resultProvidesStream() {
        RetrievalService service = new RetrievalService(indexManager);
        JVS query = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":10}}");
        RetrievalResult result = service.retrieve(new RetrievalConfig("test"), query);

        long count = result.getDocumentStream().count();
        assertThat(count).isEqualTo(5);
    }

    @Test
    @DisplayName("StreamRetriever added via builder")
    void builderAcceptsStreamStage() {
        StreamRetriever mapper = new StreamRetriever() {
            public boolean participate(JVS query, RetrievalContext ctx) { return true; }
            public Stream<JVS> retrieve(Stream<JVS> input, JVS query, RetrievalContext ctx) {
                return input.peek(doc -> doc.set("_tagged", "yes"));
            }
        };

        var pipeline = new RetrievalPipelineBuilder()
                .indexManager(indexManager)
                .disableFixup()
                .disablePagination()
                .disableFacets()
                .addStreamStage(mapper)
                .build();

        JVS query = JVS.read("{\"search\":{\"query\":\"*:*\",\"limit\":5}}");
        RetrievalContext ctx = new RetrievalContext("test", null, "en");
        var iter = pipeline.execute(query, ctx);
        var docs = iter.toCollection(new java.util.ArrayList<>());
        assertThat(docs).allSatisfy(doc ->
                assertThat(doc.getString("_tagged")).isEqualTo("yes"));
    }
}
