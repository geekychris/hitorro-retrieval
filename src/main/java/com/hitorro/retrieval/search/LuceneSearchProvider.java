package com.hitorro.retrieval.search;

import com.hitorro.index.IndexManager;
import com.hitorro.index.search.SearchResult;

import java.util.List;

/**
 * SearchProvider backed by a local hitorro-index {@link IndexManager}.
 */
public class LuceneSearchProvider implements SearchProvider {

    private final IndexManager indexManager;

    public LuceneSearchProvider(IndexManager indexManager) {
        this.indexManager = indexManager;
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang) throws Exception {
        return indexManager.search(indexName, queryString, offset, limit, facetDims, lang);
    }

    @Override
    public String getName() { return "lucene"; }

    @Override
    public boolean isAvailable() { return true; }

    public IndexManager getIndexManager() { return indexManager; }
}
