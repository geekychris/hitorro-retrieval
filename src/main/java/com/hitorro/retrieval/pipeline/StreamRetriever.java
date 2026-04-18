package com.hitorro.retrieval.pipeline;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;

import java.util.stream.Stream;

/**
 * Stream-based pipeline stage. Alternative to {@link Retriever} that works
 * with Java Streams instead of AbstractIterator.
 */
public interface StreamRetriever {

    boolean participate(JVS query, RetrievalContext context);

    Stream<JVS> retrieve(Stream<JVS> input, JVS query, RetrievalContext context);
}
