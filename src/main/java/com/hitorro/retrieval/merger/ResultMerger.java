package com.hitorro.retrieval.merger;

import com.hitorro.index.search.SearchResult;

import java.util.List;

/**
 * Merges multiple {@link SearchResult} instances from different providers
 * into a single unified result, respecting sort order.
 */
public interface ResultMerger {

    SearchResult merge(List<SearchResult> results, int offset, int limit);

    SearchResult merge(List<SearchResult> results, int offset, int limit, SortCriteria criteria);

    String getName();
}
