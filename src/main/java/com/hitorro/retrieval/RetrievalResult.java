package com.hitorro.retrieval;

import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.CollectionIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Result of a retrieval pipeline execution.
 *
 * <p>Contains the document iterator (lazy -- documents are produced as you
 * iterate), plus access to aggregates (summary, facets) and any errors.
 *
 * <h3>Unified stream ordering</h3>
 * <p>{@link #unifiedStream()} yields all results in a defined order:
 * <ol>
 *   <li>Documents ({@code _type: "doc"}) -- streamed lazily from the pipeline</li>
 *   <li>Aggregates ({@code _type: "facets"}, {@code _type: "aggregate"}) -- appended after all documents</li>
 * </ol>
 * This ordering allows consumers to display documents as they arrive, then update
 * facet navigation and summaries after the document stream completes.
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
        return documents.toCollection(new ArrayList<>());
    }

    /** Side-channel results (summary, facets). Available after documents are consumed. */
    public List<JVS> getAggregates() {
        return context.collectAggregateResults();
    }

    /**
     * Returns a unified stream that yields all results in order:
     * documents first (each tagged with {@code _type: "doc"}),
     * then aggregates at the tail (tagged with their aggregate type).
     *
     * <p>This is the primary interface for wire transport -- the consumer
     * iterates a single stream, documents flow first, then facets and
     * summaries arrive after all documents.
     */
    public AbstractIterator<JVS> unifiedStream() {
        return new UnifiedResultIterator(documents, context);
    }

    /** The underlying SearchResult from the index stage, if available. */
    public SearchResult getSearchResult() {
        return context.getSearchResult();
    }

    public boolean hasErrors() { return context.hasErrors(); }

    public List<String> getErrors() { return context.getErrors(); }

    public RetrievalContext getContext() { return context; }

    // ─── Unified iterator: docs first, then aggregates ──────────

    private static class UnifiedResultIterator extends AbstractIterator<JVS> {
        private final AbstractIterator<JVS> docIterator;
        private final RetrievalContext context;
        private Iterator<JVS> aggIterator;
        private boolean inAggPhase = false;

        UnifiedResultIterator(AbstractIterator<JVS> docIterator, RetrievalContext context) {
            this.docIterator = docIterator;
            this.context = context;
        }

        @Override
        public boolean hasNext() {
            if (!inAggPhase) {
                if (docIterator.hasNext()) return true;
                // Transition to aggregate phase
                inAggPhase = true;
                List<JVS> aggs = context.collectAggregateResults();
                for (JVS agg : aggs) {
                    // Tag with _type based on _aggregate value
                    String aggType = null;
                    try { aggType = agg.getString("_aggregate"); } catch (Exception ignored) {}
                    if ("facets".equals(aggType)) {
                        agg.set("_type", "facets");
                    } else {
                        agg.set("_type", "aggregate");
                    }
                }
                aggIterator = aggs.iterator();
            }
            return aggIterator != null && aggIterator.hasNext();
        }

        @Override
        public JVS next() {
            if (!inAggPhase) {
                JVS doc = docIterator.next();
                doc.set("_type", "doc");
                return doc;
            }
            if (aggIterator != null && aggIterator.hasNext()) {
                return aggIterator.next();
            }
            throw new NoSuchElementException();
        }
    }
}
