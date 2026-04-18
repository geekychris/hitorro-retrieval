package com.hitorro.retrieval.search;

import com.hitorro.index.search.SearchResult;
import com.hitorro.retrieval.merger.ResultMerger;
import com.hitorro.retrieval.merger.ScoreMerger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SearchProvider that fans out a query to multiple providers and merges
 * results using a {@link ResultMerger}.
 */
public class CompositeSearchProvider implements SearchProvider {

    private final List<SearchProvider> providers;
    private final ResultMerger merger;

    public CompositeSearchProvider(List<SearchProvider> providers, ResultMerger merger) {
        this.providers = List.copyOf(providers);
        this.merger = merger != null ? merger : new ScoreMerger();
    }

    public CompositeSearchProvider(List<SearchProvider> providers) {
        this(providers, new ScoreMerger());
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang) throws Exception {
        List<SearchResult> results = new ArrayList<>();
        List<Exception> errors = new ArrayList<>();

        for (SearchProvider provider : providers) {
            if (!provider.isAvailable()) continue;
            try {
                // Each provider gets the full limit; merger handles pagination
                SearchResult sr = provider.search(indexName, queryString, 0, offset + limit, facetDims, lang);
                results.add(sr);
            } catch (Exception e) {
                errors.add(e);
            }
        }

        if (results.isEmpty()) {
            if (!errors.isEmpty()) throw errors.get(0);
            return SearchResult.builder()
                    .documents(List.of())
                    .totalHits(0)
                    .query(queryString)
                    .offset(offset)
                    .limit(limit)
                    .build();
        }

        return merger.merge(results, offset, limit);
    }

    @Override
    public String getName() {
        return "composite[" + providers.stream()
                .map(SearchProvider::getName)
                .collect(Collectors.joining(",")) + "]";
    }

    @Override
    public boolean isAvailable() {
        return providers.stream().anyMatch(SearchProvider::isAvailable);
    }
}
