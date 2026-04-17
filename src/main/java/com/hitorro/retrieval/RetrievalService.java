package com.hitorro.retrieval;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.index.IndexManager;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.kvstore.TypedKVStore;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.RetrievalPipeline;
import com.hitorro.retrieval.pipeline.RetrievalPipelineBuilder;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point for the retrieval module.
 *
 * <p>Constructs and executes retrieval pipelines against named indexes,
 * optionally enriching results from a RocksDB document store.
 *
 * <pre>
 * // Index-only retrieval
 * RetrievalService service = new RetrievalService(indexManager);
 *
 * // With KVStore for full document fetch
 * RetrievalService service = new RetrievalService(indexManager, documentStore);
 *
 * // Execute a query
 * JVS query = JVS.read("""
 *     {"search": {"query": "title.mls:climate", "limit": 10, "facets": ["department"]}}
 *     """);
 * RetrievalResult result = service.retrieve(config, query);
 * List&lt;JVS&gt; docs = result.getDocumentList();
 * List&lt;JVS&gt; aggregates = result.getAggregates();
 * </pre>
 */
public class RetrievalService {

    private final IndexManager indexManager;
    private final TypedKVStore<JsonNode> documentStore;
    private final List<Retriever> customStages = new ArrayList<>();

    public RetrievalService(IndexManager indexManager, TypedKVStore<JsonNode> documentStore) {
        this.indexManager = indexManager;
        this.documentStore = documentStore;
    }

    public RetrievalService(IndexManager indexManager) {
        this(indexManager, null);
    }

    /** Register a custom pipeline stage that will be appended to every pipeline. */
    public RetrievalService addCustomStage(Retriever stage) {
        customStages.add(stage);
        return this;
    }

    /**
     * Execute a retrieval query against the given config.
     *
     * @param config the retrieval target (index name, type, language)
     * @param query  the retrieval query (JVS with search/fetch/fixup/page sections)
     * @return a RetrievalResult containing the document iterator and aggregates
     */
    public RetrievalResult retrieve(RetrievalConfig config, JVS query) {
        RetrievalContext context = new RetrievalContext(
                config.getIndexName(), config.getType(), config.getDefaultLang());

        RetrievalPipelineBuilder builder = new RetrievalPipelineBuilder()
                .indexManager(indexManager);

        if (documentStore != null) {
            builder.documentStore(documentStore);
        }

        for (Retriever stage : customStages) {
            builder.addStage(stage);
        }

        RetrievalPipeline pipeline = builder.build();
        AbstractIterator<JVS> documents = pipeline.execute(query, context);

        return new RetrievalResult(documents, context);
    }
}
