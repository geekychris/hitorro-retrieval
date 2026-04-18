package com.hitorro.retrieval.pipeline;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.CollectionIterator;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wraps a standard {@link Retriever} as a {@link StreamRetriever} for use
 * in stream-based pipelines.
 */
public class RetrieverToStreamAdapter implements StreamRetriever {

    private final Retriever delegate;

    public RetrieverToStreamAdapter(Retriever delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        return delegate.participate(query, context);
    }

    @Override
    public Stream<JVS> retrieve(Stream<JVS> input, JVS query, RetrievalContext context) {
        AbstractIterator<JVS> iterInput = null;
        if (input != null) {
            List<JVS> collected = input.collect(Collectors.toList());
            iterInput = new CollectionIterator<>(collected);
        }
        AbstractIterator<JVS> output = delegate.retrieve(iterInput, query, context);
        if (output == null) return Stream.empty();
        Iterable<JVS> iterable = () -> output;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
