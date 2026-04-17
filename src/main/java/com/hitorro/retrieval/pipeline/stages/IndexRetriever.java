package com.hitorro.retrieval.pipeline.stages;

import com.hitorro.index.IndexManager;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.aggregate.SearchSummaryAggregate;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.CollectionIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes the Lucene search via {@link IndexManager} and produces the initial
 * document iterator.
 *
 * <p>This single stage replaces the old SolrRetriever plus the entire QueryVisitor
 * chain (Query, Filter, Select, Sort, Rows, Facets, Debug, Group). The hitorro-index
 * module's {@code JVSLuceneSearcher} handles query parsing with JVS field path
 * resolution, faceting, and language-specific analysis natively.
 *
 * <h3>Query format</h3>
 * <pre>
 * {
 *   "search": {
 *     "query": "title.mls:climate AND department:Research",
 *     "offset": 0,
 *     "limit": 20,
 *     "facets": ["department", "classification"],
 *     "lang": "en"
 *   }
 * }
 * </pre>
 */
public class IndexRetriever implements Retriever {

    private static final Logger log = LoggerFactory.getLogger(IndexRetriever.class);
    private final IndexManager indexManager;

    public IndexRetriever(IndexManager indexManager) {
        this.indexManager = indexManager;
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

            // Language from query overrides context default
            String lang = context.getLang();
            if (query.exists("search.lang")) {
                lang = query.getString("search.lang");
            }

            SearchResult result = indexManager.search(
                    context.getIndexName(), queryString, offset, limit, facetDims, lang);

            context.setSearchResult(result);
            context.addAggregate(new SearchSummaryAggregate());

            return new CollectionIterator<>(result.getDocuments());

        } catch (Exception e) {
            log.error("Index search failed", e);
            context.addError("Index search failed: " + e.getMessage());
            return new CollectionIterator<>(List.of());
        }
    }

    private int getInt(JVS query, String path, int defaultValue) {
        try {
            if (query.exists(path)) {
                return (int) query.getLong(path);
            }
        } catch (Exception ignored) {}
        return defaultValue;
    }

    private List<String> getStringList(JVS query, String path) {
        try {
            if (query.exists(path)) {
                return query.getStringList(path);
            }
        } catch (Exception ignored) {}
        return new ArrayList<>();
    }
}
