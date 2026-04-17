package com.hitorro.retrieval.pipeline;

import com.hitorro.index.IndexManager;
import com.hitorro.kvstore.TypedKVStore;
import com.hitorro.retrieval.pipeline.stages.*;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for constructing a {@link RetrievalPipeline}.
 *
 * <p>Stages are added in a fixed, well-defined order. The builder also
 * accepts custom stages that are appended after the built-in ones.
 *
 * <pre>
 * RetrievalPipeline pipeline = new RetrievalPipelineBuilder()
 *     .indexManager(indexManager)
 *     .documentStore(kvStore)
 *     .build();
 * </pre>
 */
public class RetrievalPipelineBuilder {

    private IndexManager indexManager;
    private TypedKVStore<JsonNode> documentStore;
    private boolean enableFixup = true;
    private boolean enablePagination = true;
    private boolean enableFacets = true;
    private final List<Retriever> customStages = new ArrayList<>();

    /** Required. The IndexManager used for Lucene search. */
    public RetrievalPipelineBuilder indexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
        return this;
    }

    /** Optional. RocksDB store for fetching full documents. */
    public RetrievalPipelineBuilder documentStore(TypedKVStore<JsonNode> store) {
        this.documentStore = store;
        return this;
    }

    public RetrievalPipelineBuilder disableFixup() {
        this.enableFixup = false;
        return this;
    }

    public RetrievalPipelineBuilder disablePagination() {
        this.enablePagination = false;
        return this;
    }

    public RetrievalPipelineBuilder disableFacets() {
        this.enableFacets = false;
        return this;
    }

    /** Append a custom stage after the built-in stages. */
    public RetrievalPipelineBuilder addStage(Retriever stage) {
        customStages.add(stage);
        return this;
    }

    public RetrievalPipeline build() {
        if (indexManager == null) {
            throw new IllegalStateException("IndexManager is required");
        }

        List<Retriever> stages = new ArrayList<>();

        // Stage 1: Index search (always first, always required)
        stages.add(new IndexRetriever(indexManager));

        // Stage 2: Document retrieval from KVStore (optional)
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

        // Custom stages appended at the end
        stages.addAll(customStages);

        return new RetrievalPipeline(stages);
    }
}
