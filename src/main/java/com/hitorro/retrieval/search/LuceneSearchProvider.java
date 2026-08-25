package com.hitorro.retrieval.search;

import com.hitorro.index.IndexManager;
import com.hitorro.index.search.JVSLuceneSearcher;
import com.hitorro.index.search.SearchResult;
import com.hitorro.retrieval.merger.SortCriteria;
import org.apache.lucene.search.Sort;

import java.util.ArrayList;
import java.util.List;

/**
 * SearchProvider backed by a local hitorro-index {@link IndexManager}.
 *
 * <p>Sort translation: the {@link SortCriteria} chain passed on the
 * sort-aware overload is walked into parallel {@code fields[]} +
 * {@code reverses[]} arrays, then handed to
 * {@link JVSLuceneSearcher#buildLuceneSort} which resolves each field's
 * {@code DocValuesType} on the refreshed reader and produces the right
 * {@code SortField} flavour (NUMERIC → LONG, SORTED_SET → SortedSet,
 * etc.). Fields with no docvalues are silently skipped there — the
 * resulting {@code Sort} may end up null, in which case Lucene falls
 * back to score order.</p>
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
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang,
                               SortCriteria sortCriteria) throws Exception {
        Sort sort = toLuceneSort(indexName, sortCriteria);
        return indexManager.search(indexName, queryString, offset, limit, facetDims, lang, sort);
    }

    /**
     * Walk the {@link SortCriteria} chain and hand the (field, reverse)
     * pairs to the searcher's DocValues-aware sort builder. Returns null
     * when the chain is null, empty, or a single byScore() (Lucene's
     * default order — no comparator needed).
     */
    private Sort toLuceneSort(String indexName, SortCriteria criteria) throws Exception {
        if (criteria == null) return null;
        // byScore()-only chain — Lucene's default; skip building a Sort.
        if (criteria.next() == null
                && criteria.type() == SortCriteria.SortType.SCORE
                && criteria.direction() == SortCriteria.Direction.DESC) {
            return null;
        }

        List<String>  fieldList   = new ArrayList<>();
        List<Boolean> reverseList = new ArrayList<>();
        for (SortCriteria n = criteria; n != null; n = n.next()) {
            if (n.field() == null || n.field().isBlank()) continue;
            fieldList.add(n.field());
            reverseList.add(n.direction() == SortCriteria.Direction.DESC);
        }
        if (fieldList.isEmpty()) return null;

        IndexManager.IndexHandle handle = indexManager.getIndex(indexName);
        if (handle == null) return null;
        handle.refreshSearcher();

        String[]  fields   = fieldList.toArray(new String[0]);
        boolean[] reverses = new boolean[reverseList.size()];
        for (int i = 0; i < reverses.length; i++) reverses[i] = reverseList.get(i);

        return handle.getSearcher().buildLuceneSort(fields, reverses);
    }

    @Override
    public String getName() { return "lucene"; }

    @Override
    public boolean isAvailable() { return true; }

    public IndexManager getIndexManager() { return indexManager; }
}
