package com.hitorro.retrieval.context;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.retrieval.aggregate.RetrievalAggregate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mutable context carrying state between pipeline stages.
 *
 * <p>Created once per retrieval execution and shared across all stages.
 * Holds the index name, type, language, the Lucene SearchResult (set by
 * IndexRetriever), collected aggregates, and any errors encountered.
 */
public class RetrievalContext {

    private final String indexName;
    private final Type type;
    private final String lang;

    private SearchResult searchResult;
    private final List<RetrievalAggregate> aggregates = new ArrayList<>();
    private final List<String> errors = new ArrayList<>();
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public RetrievalContext(String indexName, Type type, String lang) {
        this.indexName = indexName;
        this.type = type;
        this.lang = lang != null ? lang : "en";
    }

    public String getIndexName() { return indexName; }

    public Type getType() { return type; }

    public String getLang() { return lang; }

    public void setSearchResult(SearchResult searchResult) {
        this.searchResult = searchResult;
    }

    public SearchResult getSearchResult() { return searchResult; }

    public void addAggregate(RetrievalAggregate aggregate) {
        if (aggregate != null) {
            aggregates.add(aggregate);
        }
    }

    public List<RetrievalAggregate> getAggregates() {
        return aggregates;
    }

    /**
     * Materializes all registered aggregates as JVS documents.
     * Null results from individual aggregates are filtered out.
     */
    public List<JVS> collectAggregateResults() {
        List<JVS> results = new ArrayList<>();
        for (RetrievalAggregate agg : aggregates) {
            JVS jvs = agg.toJVS(this);
            if (jvs != null) {
                results.add(jvs);
            }
        }
        return results;
    }

    public void addError(String error) {
        if (error != null) {
            errors.add(error);
        }
    }

    public List<String> getErrors() { return errors; }

    public boolean hasErrors() { return !errors.isEmpty(); }

    // ─── Typed Attributes (inter-stage communication) ────────────

    public void setAttribute(String key, Object value) {
        if (key != null && value != null) {
            attributes.put(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, Class<T> type) {
        Object val = attributes.get(key);
        if (val != null && type.isInstance(val)) {
            return (T) val;
        }
        return null;
    }

    public <T> Optional<T> getOptionalAttribute(String key, Class<T> type) {
        return Optional.ofNullable(getAttribute(key, type));
    }

    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }
}
