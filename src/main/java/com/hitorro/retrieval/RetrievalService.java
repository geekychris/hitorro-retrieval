package com.hitorro.retrieval;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.index.IndexManager;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.datamapper.AIOperations;
import com.hitorro.kvstore.TypedKVStore;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.docstore.DocumentStore;
import com.hitorro.retrieval.docstore.LocalKVDocumentStore;
import com.hitorro.retrieval.pipeline.RetrievalPipeline;
import com.hitorro.retrieval.pipeline.RetrievalPipelineBuilder;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.retrieval.pipeline.StreamRetriever;
import com.hitorro.retrieval.search.LuceneSearchProvider;
import com.hitorro.retrieval.search.SearchProvider;
import com.hitorro.util.core.iterator.AbstractIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point for the retrieval module.
 *
 * <p>Constructs and executes retrieval pipelines with pluggable search providers,
 * document stores, result mergers, and AI summarization.
 */
public class RetrievalService {

    private final SearchProvider searchProvider;
    private final DocumentStore documentStore;
    private final List<Retriever> customStages = new ArrayList<>();
    private AIOperations aiOperations;

    public RetrievalService(SearchProvider searchProvider, DocumentStore documentStore) {
        this.searchProvider = searchProvider;
        this.documentStore = documentStore;
    }

    public RetrievalService(SearchProvider searchProvider) {
        this(searchProvider, null);
    }

    /** Backward-compatible: wraps IndexManager + TypedKVStore. */
    public RetrievalService(IndexManager indexManager, TypedKVStore<JsonNode> kvStore) {
        this(new LuceneSearchProvider(indexManager),
             kvStore != null ? new LocalKVDocumentStore(kvStore) : null);
    }

    /** Backward-compatible: wraps IndexManager only. */
    public RetrievalService(IndexManager indexManager) {
        this(new LuceneSearchProvider(indexManager));
    }

    public RetrievalService addCustomStage(Retriever stage) {
        customStages.add(stage);
        return this;
    }

    public RetrievalService enableSummarization(AIOperations ai) {
        this.aiOperations = ai;
        return this;
    }

    public RetrievalResult retrieve(RetrievalConfig config, JVS query) {
        RetrievalContext context = new RetrievalContext(
                config.getIndexName(), config.getType(), config.getDefaultLang());

        RetrievalPipelineBuilder builder = new RetrievalPipelineBuilder()
                .searchProvider(searchProvider);

        if (documentStore != null) {
            builder.documentStore(documentStore);
        }

        if (aiOperations != null) {
            builder.enableSummarization(aiOperations);
        }

        for (Retriever stage : customStages) {
            builder.addStage(stage);
        }

        RetrievalPipeline pipeline = builder.build();
        AbstractIterator<JVS> documents = pipeline.execute(query, context);

        return new RetrievalResult(documents, context);
    }
}
