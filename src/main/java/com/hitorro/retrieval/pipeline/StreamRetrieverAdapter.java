package com.hitorro.retrieval.pipeline;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.CollectionIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wraps a {@link StreamRetriever} as a {@link Retriever} so it can participate
 * in the standard AbstractIterator-based pipeline.
 */
public class StreamRetrieverAdapter implements Retriever {

    private final StreamRetriever delegate;

    public StreamRetrieverAdapter(StreamRetriever delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        return delegate.participate(query, context);
    }

    @Override
    public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
        Stream<JVS> inputStream = (input != null) ? toStream(input) : Stream.empty();
        Stream<JVS> outputStream = delegate.retrieve(inputStream, query, context);
        List<JVS> collected = outputStream.collect(Collectors.toList());
        return new CollectionIterator<>(collected);
    }

    private static Stream<JVS> toStream(AbstractIterator<JVS> iter) {
        Iterable<JVS> iterable = () -> iter;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
