package com.hitorro.retrieval.search;

import com.hitorro.index.search.SearchResult;
import com.hitorro.retrieval.merger.SortCriteria;

import java.util.List;

/**
 * Abstraction over a search backend. Allows plugging in Lucene, remote HTTP,
 * or composite (multi-provider) search implementations.
 *
 * <p>Two search overloads:</p>
 * <ul>
 *   <li>The base {@link #search(String, String, int, int, List, String)}
 *       is what every implementation must satisfy. It's the pre-sort
 *       contract — score-desc order.</li>
 *   <li>The sort-taking {@link #search(String, String, int, int, List, String, SortCriteria)}
 *       is a default that delegates to the base method — implementations
 *       that support sort (LuceneSearchProvider, CompositeSearchProvider,
 *       ReadOnlySearchProvider) override it. Implementations that don't
 *       (RemoteSearchProvider on the pre-sort wire) inherit the default
 *       and silently drop the sort chain rather than error, since a
 *       score-ordered fallback is still a valid result.</li>
 * </ul>
 */
public interface SearchProvider {

    SearchResult search(String indexName, String queryString, int offset, int limit,
                        List<String> facetDims, String lang) throws Exception;

    /**
     * Search with a caller-requested {@link SortCriteria} chain. Default
     * implementation drops the sort and delegates to the base method —
     * implementations backed by a store that can honour sort should
     * override this instead of the base method.
     */
    default SearchResult search(String indexName, String queryString, int offset, int limit,
                                List<String> facetDims, String lang,
                                SortCriteria sortCriteria) throws Exception {
        return search(indexName, queryString, offset, limit, facetDims, lang);
    }

    String getName();

    boolean isAvailable();
}
