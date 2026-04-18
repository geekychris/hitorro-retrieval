package com.hitorro.retrieval.context;

/**
 * Predefined attribute keys for inter-stage communication via {@link RetrievalContext}.
 */
public final class ContextAttributes {
    private ContextAttributes() {}

    public static final String SORT_CRITERIA = "retrieval.sort.criteria";
    public static final String SEARCH_PROVIDERS = "retrieval.search.providers";
    public static final String SEARCH_TIME_MS = "retrieval.search.timeMs";
    public static final String TOTAL_HITS = "retrieval.totalHits";
    public static final String MERGER_USED = "retrieval.merger";
    public static final String AI_SUMMARY = "retrieval.ai.summary";
    public static final String DOCUMENT_STORE_TYPE = "retrieval.docstore.type";
}
