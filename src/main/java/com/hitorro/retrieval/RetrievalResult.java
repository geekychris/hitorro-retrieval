package com.hitorro.retrieval;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.util.core.iterator.AbstractIterator;

import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Result of a retrieval pipeline execution.
 *
 * <p>Contains the document iterator (lazy -- documents are produced as you
 * iterate), plus access to aggregates (summary, facets) and any errors.
 */
public class RetrievalResult {

    private final AbstractIterator<JVS> documents;
    private final RetrievalContext context;

    public RetrievalResult(AbstractIterator<JVS> documents, RetrievalContext context) {
        this.documents = documents;
        this.context = context;
    }

    /** Lazy document iterator. Consume this before calling getAggregates(). */
    public AbstractIterator<JVS> getDocuments() { return documents; }

    /** Returns documents as a Java Stream. */
    public Stream<JVS> getDocumentStream() {
        Iterable<JVS> iterable = () -> documents;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /** Materializes all documents into a list. */
    public List<JVS> getDocumentList() {
        return documents.toCollection(new java.util.ArrayList<>());
    }

    /** Side-channel results (summary, facets). Available after documents are consumed. */
    public List<JVS> getAggregates() {
        return context.collectAggregateResults();
    }

    /** The underlying SearchResult from the index stage, if available. */
    public SearchResult getSearchResult() {
        return context.getSearchResult();
    }

    public boolean hasErrors() { return context.hasErrors(); }

    public List<String> getErrors() { return context.getErrors(); }

    public RetrievalContext getContext() { return context; }
}
