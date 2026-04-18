package com.hitorro.retrieval.search;

import com.hitorro.index.search.SearchResult;

import java.util.List;

/**
 * Abstraction over a search backend. Allows plugging in Lucene, remote HTTP,
 * or composite (multi-provider) search implementations.
 */
public interface SearchProvider {

    SearchResult search(String indexName, String queryString, int offset, int limit,
                        List<String> facetDims, String lang) throws Exception;

    String getName();

    boolean isAvailable();
}
