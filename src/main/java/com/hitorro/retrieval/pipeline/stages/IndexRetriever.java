package com.hitorro.retrieval.pipeline.stages;

import com.hitorro.index.IndexManager;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.aggregate.SearchSummaryAggregate;
import com.hitorro.retrieval.context.ContextAttributes;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.merger.SortCriteria;
import com.hitorro.retrieval.merger.SortCriteriaParser;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.retrieval.search.LuceneSearchProvider;
import com.hitorro.retrieval.search.SearchProvider;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.CollectionIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes search via a pluggable {@link SearchProvider} and produces
 * the initial document iterator.
 */
public class IndexRetriever implements Retriever {

    private static final Logger log = LoggerFactory.getLogger(IndexRetriever.class);
    private final SearchProvider searchProvider;

    public IndexRetriever(SearchProvider searchProvider) {
        this.searchProvider = searchProvider;
    }

    /** Backward-compatible constructor wrapping IndexManager. */
    public IndexRetriever(IndexManager indexManager) {
        this(new LuceneSearchProvider(indexManager));
    }

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        return query.exists("search.query");
    }

    @Override
    public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
        try {
            String queryString = query.getString("search.query");
            int offset = getInt(query, "search.offset", 0);
            int limit = getInt(query, "search.limit", 20);
            List<String> facetDims = getStringList(query, "search.facets");

            String lang = context.getLang();
            if (query.exists("search.lang")) {
                lang = query.getString("search.lang");
            }

            // search.sort — optional composite sort chain. Accepts both
            // the array-of-{field,direction} shape the coordinator emits
            // and the compact "field:dir[,field:dir]" string the BFF /
            // remote querystring can send. Null → provider falls back to
            // score-desc (Lucene's default).
            SortCriteria sortCriteria = extractSortCriteria(query);

            SearchResult result = searchProvider.search(
                    context.getIndexName(), queryString, offset, limit, facetDims, lang, sortCriteria);

            context.setSearchResult(result);
            context.addAggregate(new SearchSummaryAggregate());
            context.setAttribute(ContextAttributes.SEARCH_PROVIDERS, searchProvider.getName());
            context.setAttribute(ContextAttributes.TOTAL_HITS, result.getTotalHits());
            context.setAttribute(ContextAttributes.SEARCH_TIME_MS, result.getSearchTimeMs());

            return new CollectionIterator<>(result.getDocuments());

        } catch (Exception e) {
            log.error("Search failed via {}", searchProvider.getName(), e);
            context.addError("Search failed (" + searchProvider.getName() + "): " + e.getMessage());
            return new CollectionIterator<>(List.of());
        }
    }

    private int getInt(JVS query, String path, int defaultValue) {
        try {
            if (query.exists(path)) return (int) query.getLong(path);
        } catch (Exception ignored) {}
        return defaultValue;
    }

    private List<String> getStringList(JVS query, String path) {
        try {
            if (query.exists(path)) return query.getStringList(path);
        } catch (Exception ignored) {}
        return new ArrayList<>();
    }

    /** Read search.sort from the query JVS in either the array or string
     *  wire shape and delegate parsing to {@link SortCriteriaParser}. */
    private static SortCriteria extractSortCriteria(JVS query) {
        try {
            if (!query.exists("search.sort")) return null;
            return SortCriteriaParser.parse(query.getJsonNode().at("/search/sort"));
        } catch (Exception ignored) {
            return null;
        }
    }
}
