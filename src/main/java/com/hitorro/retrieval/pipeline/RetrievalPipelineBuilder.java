package com.hitorro.retrieval.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.index.IndexManager;
import com.hitorro.jsontypesystem.datamapper.AIOperations;
import com.hitorro.kvstore.TypedKVStore;
import com.hitorro.retrieval.docstore.DocumentStore;
import com.hitorro.retrieval.docstore.LocalKVDocumentStore;
import com.hitorro.retrieval.pipeline.stages.*;
import com.hitorro.retrieval.search.LuceneSearchProvider;
import com.hitorro.retrieval.search.SearchProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for constructing a {@link RetrievalPipeline}.
 *
 * <p>Stages are added in a fixed, well-defined order. The builder also
 * accepts custom stages (both Retriever and StreamRetriever) appended
 * after the built-in ones.
 */
public class RetrievalPipelineBuilder {

    private SearchProvider searchProvider;
    private DocumentStore documentStore;
    private AIOperations aiOperations;
    private int summarizeMaxDocs = 10;
    private int summarizeMaxWords = 200;
    private boolean enableFixup = true;
    private boolean enablePagination = true;
    private boolean enableFacets = true;
    private boolean enableSummarization = false;
    private final List<Retriever> customStages = new ArrayList<>();

    /** Required. The search provider (Lucene, remote, composite). */
    public RetrievalPipelineBuilder searchProvider(SearchProvider provider) {
        this.searchProvider = provider;
        return this;
    }

    /** Convenience: wraps IndexManager in a LuceneSearchProvider. */
    public RetrievalPipelineBuilder indexManager(IndexManager indexManager) {
        this.searchProvider = new LuceneSearchProvider(indexManager);
        return this;
    }

    /** Optional: document store for full document fetch (local KV, remote, etc). */
    public RetrievalPipelineBuilder documentStore(DocumentStore store) {
        this.documentStore = store;
        return this;
    }

    /** Convenience: wraps TypedKVStore in a LocalKVDocumentStore. */
    public RetrievalPipelineBuilder documentStore(TypedKVStore<JsonNode> kvStore) {
        this.documentStore = new LocalKVDocumentStore(kvStore);
        return this;
    }

    /** Enable AI summarization with the given AIOperations implementation. */
    public RetrievalPipelineBuilder enableSummarization(AIOperations ai) {
        this.aiOperations = ai;
        this.enableSummarization = true;
        return this;
    }

    /** Enable AI summarization with custom document/word limits. */
    public RetrievalPipelineBuilder enableSummarization(AIOperations ai, int maxDocs, int maxWords) {
        this.aiOperations = ai;
        this.enableSummarization = true;
        this.summarizeMaxDocs = maxDocs;
        this.summarizeMaxWords = maxWords;
        return this;
    }

    public RetrievalPipelineBuilder disableFixup() { this.enableFixup = false; return this; }
    public RetrievalPipelineBuilder disablePagination() { this.enablePagination = false; return this; }
    public RetrievalPipelineBuilder disableFacets() { this.enableFacets = false; return this; }

    /** Append a custom Retriever stage after the built-in stages. */
    public RetrievalPipelineBuilder addStage(Retriever stage) {
        customStages.add(stage);
        return this;
    }

    /** Append a custom StreamRetriever stage (wrapped in an adapter). */
    public RetrievalPipelineBuilder addStreamStage(StreamRetriever stage) {
        customStages.add(new StreamRetrieverAdapter(stage));
        return this;
    }

    public RetrievalPipeline build() {
        if (searchProvider == null) {
            throw new IllegalStateException("SearchProvider is required (call searchProvider() or indexManager())");
        }

        List<Retriever> stages = new ArrayList<>();

        // Stage 1: Search (always first)
        stages.add(new IndexRetriever(searchProvider));

        // Stage 2: Document fetch from store (optional)
        if (documentStore != null) {
            stages.add(new DocumentRetriever(documentStore));
        }

        // Stage 3: Field fixup / enrichment (optional)
        if (enableFixup) {
            stages.add(new FixupRetriever());
        }

        // Stage 4: Client-side pagination (optional)
        if (enablePagination) {
            stages.add(new PaginationRetriever());
        }

        // Stage 5: Facet aggregate registration (optional)
        if (enableFacets) {
            stages.add(new FacetRetriever());
        }

        // Stage 6: AI Summarization (optional, after pagination so it summarizes the final page)
        if (enableSummarization && aiOperations != null) {
            stages.add(new SummarizationRetriever(aiOperations, summarizeMaxDocs, summarizeMaxWords));
        }

        // Custom stages appended last
        stages.addAll(customStages);

        return new RetrievalPipeline(stages);
    }
}
