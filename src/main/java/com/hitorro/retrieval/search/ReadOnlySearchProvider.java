/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.search;

import com.hitorro.index.readonly.ReadOnlyIndexService;
import com.hitorro.index.search.JVSLuceneSearcher;
import com.hitorro.index.search.SearchResult;
import com.hitorro.retrieval.merger.SortCriteria;
import org.apache.lucene.search.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * {@link SearchProvider} backed by {@link ReadOnlyIndexService}.
 * Bypasses {@code IndexManager} + {@code LuceneSearchProvider} because
 * the former grabs {@code write.lock} on every index open — fine for
 * standalone mode but incompatible with concurrent writers in shared
 * mode.
 *
 * <p>Lives in {@code hitorro-retrieval} — this is the natural home
 * since it implements the {@link SearchProvider} interface owned by
 * this module and depends on {@link ReadOnlyIndexService} from
 * {@code hitorro-index}. Any Spring Boot service or standalone tool
 * that wants to serve read-only retrieval alongside a live writer can
 * use it directly.</p>
 *
 * <p>Sort translation mirrors {@link LuceneSearchProvider} — the
 * {@link SortCriteria} chain is unrolled into (field, reverse) pairs
 * and handed to {@link JVSLuceneSearcher#buildLuceneSort} on the
 * refreshed searcher.</p>
 */
public class ReadOnlySearchProvider implements SearchProvider {

    private final ReadOnlyIndexService indexes;
    private final Function<String, java.nio.file.Path> dirResolver;

    public ReadOnlySearchProvider(ReadOnlyIndexService indexes,
                                  Function<String, java.nio.file.Path> dirResolver) {
        this.indexes = indexes;
        this.dirResolver = dirResolver;
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang) throws Exception {
        return search(indexName, queryString, offset, limit, facetDims, lang, null);
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang,
                               SortCriteria sortCriteria) throws Exception {
        JVSLuceneSearcher s = getRefreshed(indexName);
        Sort sort = toLuceneSort(s, sortCriteria);
        return s.search(queryString, offset, limit, facetDims, lang, sort);
    }

    private JVSLuceneSearcher getRefreshed(String indexName) throws Exception {
        JVSLuceneSearcher s = indexes.get(indexName);
        if (s == null) {
            java.nio.file.Path dir = dirResolver.apply(indexName);
            if (dir == null) throw new IllegalArgumentException("Unknown index: " + indexName);
            s = indexes.openOrGet(indexName, dir);
        } else {
            // Pick up any writes that landed since the last query.
            s = indexes.refresh(indexName);
        }
        return s;
    }

    private static Sort toLuceneSort(JVSLuceneSearcher searcher, SortCriteria criteria) throws Exception {
        if (criteria == null) return null;
        // byScore()-only chain matches Lucene's default; no comparator needed.
        if (criteria.next() == null
                && criteria.type() == SortCriteria.SortType.SCORE
                && criteria.direction() == SortCriteria.Direction.DESC) {
            return null;
        }
        List<String>  fields   = new ArrayList<>();
        List<Boolean> reverses = new ArrayList<>();
        for (SortCriteria n = criteria; n != null; n = n.next()) {
            if (n.field() == null || n.field().isBlank()) continue;
            fields.add(n.field());
            reverses.add(n.direction() == SortCriteria.Direction.DESC);
        }
        if (fields.isEmpty()) return null;
        boolean[] rev = new boolean[reverses.size()];
        for (int i = 0; i < rev.length; i++) rev[i] = reverses.get(i);
        return searcher.buildLuceneSort(fields.toArray(new String[0]), rev);
    }

    @Override public String getName()     { return "read-only-lucene"; }
    @Override public boolean isAvailable(){ return true; }
}
